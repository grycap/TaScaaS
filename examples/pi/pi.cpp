
#include <random>
#include <chrono>

#include "ruperLB.hh"


inline void sample(std::mt19937& gen,
		   const unsigned long long niter,
		   unsigned long long& in){
  long long unsigned inAux = 0;
  std::uniform_real_distribution<float> dis(0.0,1.0);
  for(unsigned long long i = 0; i < niter; ++i){
    float x = dis(gen);
    float y = dis(gen);
    float r2 = x*x + y*y;
    if(r2 <= 1.0)
      ++inAux;
  }
  in += inAux;
}

int main(int argc, char** argv){
    
  if(argc < 3){
    printf("usage: %s worker url niterations balanceInterval(s)\n",argv[0]);
    return 1;
  }
    
  //Parse parameters
  int iworker = atoi(argv[1]);
  const char* url = argv[2];
  long long int niter = std::max(atoll(argv[3]),1ll);
  long long int LBinterval = std::max(atoll(argv[4]),1ll);

  if(iworker < 0){
    printf("Error: Invalid worker index %d\n",iworker);
    return 1;
  }

  printf("Balance interval: %lld s\n",LBinterval);

  const unsigned verbose = 3;
  
  //Create the local task
  LB::task task;

  task.setCheckTime(LBinterval);
  task.minTime(LBinterval/2);
  task.init(1,niter,"local-balance-rep",verbose);
  //Init external balance
  task.extHTTPserver(iworker,url,verbose);

  //Create random generator
  std::random_device rd;
  std::mt19937 gen(rd());

  //Start local worker
  task.workerStart(0,verbose);

  //Get number of iterations to do
  unsigned long long toDo = task.assigned(0);
  
  //Calculate next report timestamp
  std::chrono::steady_clock::time_point treport =
    std::chrono::steady_clock::now() + std::chrono::seconds(LBinterval);  
  
  unsigned long long done = 0;
  const unsigned long long chunkSize = 100;
  unsigned long long in = 0;
  
  //Calculation loop
  for(;;){
    // *** Calculation function ***
    sample(gen,chunkSize,in);
    // ****************************
    done += chunkSize;

    //Check if is time to report
    std::chrono::steady_clock::time_point tnow = std::chrono::steady_clock::now();
    if(treport < tnow){
      treport = std::chrono::steady_clock::now() + std::chrono::seconds(LBinterval);
      int err;
      task.report(0,done,&err,verbose);
      task.checkPoint(verbose);

      //Update assignation
      toDo = task.assigned(0);
    }

    //Check if the number of iterations have been completed
    if(toDo <= done){
      int err;
      task.report(0,done,&err,verbose);
      int why;
      if(task.workerFinish(0,why,verbose)){
	printf("Sampled: %llu\n",done);
	printf("In: %llu\n",in);
	printf("Local estimation: %.15f\n",
	       4.0*static_cast<double>(in)/static_cast<double>(done));
	return 0;
      }
      else{
	switch(why){
	case 0: //Update assigned histories
	  toDo = task.assigned(0);
	  break;
	case 1: //Checkpoint required
	  task.checkPoint(verbose);
	  toDo = task.assigned(0);
	  break;
	case 2: //Waiting response
	  toDo += 3*chunkSize;
	  break;
	default: //Unexpected response
	  toDo += 3*chunkSize;	  
	}
      }
    }
  }
  
  return 0;
}

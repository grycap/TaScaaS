# Examples

TaScaaS package provides two different examples. The first one, located at *examples/pi*, uses the load balance client of [RUPER-LB](https://github.com/PenRed/RUPER-LB) and require no input data. Its execution estimates the number **PI** using a Monte Carlo simulation. This is a simplified version of the example provided in the [RUPER-LB](https://github.com/PenRed/RUPER-LB) package, where both multi-threading and MPI capabilities have been removed to run a single thread. However, the original example with these capabilites can be used adding the corresponding line to enable the HTTPS load balance.

```
  //Init external balance
  task.extHTTPserver(iworker,url,verbose);
```

The second example consists of a file processing application where each worker performs a fixed number of iterations, i.e. the process is not balanced. Therefore, each iteration consists of a single file to be processed. Each file contains a single column with numbers that must be factorised to determine the number of prime numbers. 

Following, it will be explained how to prepare and run both examples. The only assumption is that the TaScaaS infrastructure has been previously deployed. To handle the comunication with the API gateway, the examples use the provided *worker.py* script. This one, registers our worker infrastructure and handles the comunication with TaScaaS. By default, it asumes that the scaling process is trivial, i.e. the reserved slots are ready to use inmideatly. This approach could be used by worker infrastructures consisting of a single node, where, for example, each slot correspond to a single core of the processor. However, if our worker infrastructure consists of several working nodes, the *scale* function should be reimplemented to fit the specific infrastructure needs. The default *scale* function is shown below,

```python
def scale(reqCap,s,maxS):
    s2 = min(int(math.ceil(float(reqCap)*float(maxS))),maxS)
    return max(s2,1)
```

where **reqCap** is the required capacity percentage received from TaScaaS, **s** is the actual number of slots and **maxS** is the maximum number of slots to be allocated. As result, the function returns the new number of available slots.

Once our scale function has been implemented, the infrastructure worker daemon can be started in the worker infrastructure frontend using the command

```
python3 worker.py api-url slots max-slots sleep-time worker-executable secret
```

where **api-url** is the one provided by the serverless framework output, including the stage. For example, if the stage is *test*, and the infrastructure has been deployed in the *us-east-1* region, the api-url should be

```
https://**********.execute-api.us-east-1.amazonaws.com/test
```

where asterisks must be replaced by our api ID. **slots** and **max-slots** correspond to the number of available slots and the maximum reacheable slots by the worker infrastructure. As the provided examples are intended to run on a single computer, **max-slots** could be set to the number of available CPUs. Then, **sleep-time** is the elapsed time, in seconds, between update requests. For both examples 20 seconds should be ok. For the parameter **worker-executable** we must specify the executable which will launch the execution on each example. Finally, the **secret** parameter is the one set during the TaScaaS deployment.

## Pi
First, to run the **PI** example, the provided source code must be compiled. To do that, a CMakeLists file is provided in the example folder (*examples/pi*). This one will download the RUPER-LB package, because we use its balance client via the *task* class. Thus, to compile it, create a build folder in the example directory and go in,

```
cd examples/pi
mkdir build
cd build
```

Now, configure the cmake files and run make to compile the code,

```
cmake ../
make
```

After the successfully compilation, an executable named *pi* should appear in the *build* folder. Go to *examples/pi* directory and copy this executable and the bash script *pi-launch* to the *worker* folder,

```
cp build/pi pi-launch ../../worker/
```

Notice that both the binary (*pi*) and the script (*pi-launch*) must be executable. If they have not execution permissions, add it using:

```bash
chmod +x pi
chmod +x pi-launch
```

At this point, the program is prepared to run. The next step consists of register the computer as a worker infrastructure to accept jobs. This can be easily done using the *worker.py* script provided in the *worker* folder, as explained before. For the parameter **worker-executable** we must specify the *pi-launch* script. Finally, the **secret** parameter is the one set during the TaScaaS deployment.

Once started, the worker infrastructure daemon will periodically update its state, request jobs, and update the number of slots. To create some tasks to be processed, upload the configuration file *examples/pi/piTest.config* to the input folder of the TaScaaS S3 bucket. The configuration parameters can be changed to experiment with the infrastructure behaviour.

After that final step, the daemon will request the generated jobs to be processed. Notice that the job request could be delayed due the **sleep-time** parameter. Once jobs have been received, the worker daemon should show an output like,

```
ID: 25d2c24e-5403-11eb-9dd7-dfb04d699623
scaleTime: 200.000000
Starting 2 jobs
Starting simulation
Starting simulation
Send update petition
.
.
.
```

where the ID is the assigned uuid to the working infrastructure. In addition, some folders will be created in the *worker.py* execution folder where the simulations will be carried out. Notice that the *pi-launch* script will upload the results to the output folder of the TaScaaS S3 bucket and, then, will remove the local results. If you want to conserve the simulation files, comment the last line in the *pi-launch* script,

```
#Remove local files
rm -r sim-$ID-$iworker &> /dev/null
```

When a worker finishes the execution, we can download its results from the S3 bucket. We can also check the execution process of each worker in the DynamoDB table.

## File processing

This example, located at *examples/prime*, does not require any load balance. Thus, it is not necessary to use the RUPER-LB client to perform the execution. Instead, the communication with the server will be handled by the *processFiles-launch* script via the curl command. So, like in the **PI** example, ensure that both scripts *processFile* and *processFile-launch* have execution permissions. Then, copy both scripts to the *worker* directory,

```
cp examples/prime/processFile examples/prime/processFiles-launch worker/
```

Now, we require some data to process. To create that data, the package provides a bash script *examples/prime/genFiles.sh* to generate files with "random" big numbers to be factorised. Simply go to the example folder and run the script specifying the number of files and the number of numbers in each file,

```
cd examples/prime
bash genFiles.sh 20 100000
```

Once completed, enter to the created folder *genFiles* and create a tarball with all files,

```bash
cd genFiles
tar -cJvf files.tar.xz files_*
```

Then, upload the tarball *files.tar.xz* to the input folder of the TaScaaS S3 bucket. The final data key should be *input/files.tar.xz*. Now, to begin the execution, first, start the worker daemon as we done in the **PI** example,

```bash
python3 worker.py api-url slots max-slots sleep-time worker-executable secret
```

where, in this case, **worker-executable** will be *processFiles-launch*. Finally, upload the configuration file located at *examples/prime/filesTest.config* to the input folder of the TaScaaS S3 bucket. Notice that the number of iterations must match with the number of files to be processed. As the provided configuration file has a negative *time* parameter, no load balance is expected. Like the previous example, the results will be uploaded to the output folder of the S3 bucket.

import json
import os
import stat
import boto3
from botocore.exceptions import ClientError
from subprocess import Popen, PIPE, STDOUT
import sys
import time as tsleep
from decimal import *

dirLBexe='bin/LBserver'

def loadTable(globItem,workers,telaps):
    
    #Get required parameters
    assigned = int(globItem['assigned'])
    expectedTime = int(globItem['dt'])
    threshold = int(globItem['done'])
    #Init server
    p = Popen([dirLBexe,                    #Executable
                "1000",                     #Iterations to do
                "1",                        #Init workers
                str(threshold),             #Threshold
                str(-telaps)                #Time to move
                #,str("/tmp/serverlog.txt")  #log file
                ],              
                stdout=PIPE, stdin=PIPE)
    
    #Load server state from stdin
    p.stdin.write(b'4 stdin\n') 
    p.stdin.flush()

    #Global state information
    line = "%d %d %d %d %d 1 %d\n" % (assigned,assigned,expectedTime,threshold,len(workers),int(globItem['finished']))
    p.stdin.write(line.encode('utf-8')) 
    
    #Workers inits
    for worker in workers:
        p.stdin.write(b'%d\n' % (worker['tinit'])) 
        
    #Workers resumes
    for worker in workers:
        p.stdin.write(b'%d\n' % (worker['tresume'])) 
    
    #Workers data
    for worker in workers:
        tinit = worker['tinit']
        if "measures" in worker:
            nmeasures = len(worker['measures'])
        else:
            nmeasures = 0
            
        if tinit < 0:
            started = 0
        else:
            started = 1
        p.stdin.write(b'%d %d %d %d %d %d\n' % 
        (worker['assigned'],started,worker['finished'],worker['done'],
         worker['dt'],nmeasures)) 
    
        #Print measures
        if nmeasures > 0:
            for measure in worker['measures']:
                p.stdin.write(b'%d %E\n' % (measure['t'],measure['s']))
    
    #Flush stdin
    p.stdin.flush()
    
    return p

def createWorkers(ID, nworkers, newWorkers, table, reportTime, ndone):
    
    if nworkers <= 0:
        return
    #Create new workers
    MAXWORKERS = int(os.environ['MAXWORKERS'])
    maxNewWorkers = max(MAXWORKERS-nworkers,0)
    newWorkers = min(newWorkers,maxNewWorkers)
    if newWorkers > 0:
        print("Create %d new workers" % newWorkers)
        try:
            for i in list(range(newWorkers)):
                table.put_item(
                    Item={
                        'id': ID,
                        'worker': int(nworkers + i),
                        'tinit': int(-1),
                        'tresume': int(-1),
                        'dt': int(0),
                        'assigned': int(ndone),
                        'finished': int(0),
                        'done': int(0),
                        'measures': []
                    }
                )
                
            #Get required environment variables
            BUCKETOUT = str(os.environ['BUCKETOUT'])
            BUCKETIN = str(os.environ['BUCKETIN'])
            PREFIXOUT = str(os.environ['PREFIXOUT'])
            PREFIXIN = str(os.environ['PREFIXIN'])
            
            #Get the configuration file name
            configSplit = ID.split('_')
            del configSplit[0]
            configFile = "_".join(configSplit)
            
            #Create the key for configuration file
            keyConfig = PREFIXIN + "/" + configFile

            try:
                #Download the configuration file which cotains the data url
                s3_resource = boto3.resource('s3')
                configText = s3_resource.Object(BUCKETIN,keyConfig).get()['Body'].read().decode('utf-8')
                
                #Parse configuration json
                configuration = json.loads(configText)
                
                #Get intput data file
                if "inputFile" not in configuration:
                    print("Error: Field 'inputFile' not found at provided configuration")
                    return                
                
                #Create the key for input data
                inputFile=str(configuration["inputFile"])
                
            except ClientError as e:
                print("Error: Unable to download and parse configuration file: %s" % keyConfig)
                print("       Error: %s" % e.response["Error"]["Message"])
                return 
            
            #Create the data file key
            dataKey = PREFIXIN + "/" + inputFile
            
            #Create the common part of the configuration file
            configCommon = ID + " " + dataKey + " " + str(reportTime) + " "

            #Create the common key for configuration files
            keyConfigCommon = PREFIXOUT + "/" + ID + "_"

            #Create the configuration files at S3
            s3_client = boto3.client('s3')
            for i in list(range(newWorkers)):
                workerID = str(nworkers + i)
                workerConfig = configCommon + workerID + " " + str(ndone)
                workerKeyConfig = keyConfigCommon + workerID
                s3_client.put_object(Body=workerConfig,Bucket=BUCKETOUT, Key=workerKeyConfig)
                            
                                
        except ClientError as e:
            print("Error: Unable to create new workers at dynamoDB table and S3")
            print("       Error: %s" % e.response["Error"]["Message"])
    

def checkOutput(ID,iw,p,ndone=0,dt=0,updateTresume=False,updateTinit=False,tinit=-1,finished=False,targetETA=1000000000000, nworkers = 0, reportTime=60):

    #Get dynamo table
    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])

    #Get worker last measure
    p.stdin.write(b'6 %d\n' % (iw)) 
    p.stdin.flush()

    #Close server
    p.stdin.write(b'0\n')
    p.stdin.flush()
    
    stdout_data = p.stdout.read().decode('utf-8')
    
    lines = stdout_data.strip('\n').split('\n')
    if len(lines) < 1:
        print("Error loading state: Unexpected error")
        return {
            'statusCode': 400,
            'body': "Error loading state:\n Unexpected error"
        }
        
    #Check load state operation
    loadStateLine = lines[0]
    if loadStateLine.strip() != "0":
        #Error on state load
        print("Error loading state:")
        print(stdout_data)
        return {
            'statusCode': 400,
            'body': "Error loading state:\n %s" % stdout_data
        }

    if len(lines) < 5 or len(lines) > 7:
        #Error on state load
        print("Error loading state: Unexpected error")
        print(stdout_data)
        return {
            'statusCode': 400,
            'body': "Error loading state: Unexpected error\n %s" % stdout_data
        }
        
    resultsLines = lines[1:-3]
    measureLines = lines[-3:]
        
    if resultsLines[0].strip() == "0":
        #Successfully call, save last measure
        if measureLines[0].strip() != "0":
            #Unexpected error reading last measure
            print("Error reading last measure:")
            print(stdout_data)
            return {
                'statusCode': 400,
                'body': "Error reading last measure: %s" % measureLines
            }            
        else:
            try:
                
                #Get time and speed
                t = int(measureLines[1].strip().split()[1])
                s = float(measureLines[2].strip().split()[1])
                tremaining = 850-t
                outOfTime=False
                
                #Get dynamo client
                dynamodb = boto3.resource('dynamodb')
                #Get the table
                table = dynamodb.Table(DYNAMOTABLE)
                #Prepare the measure to add
                updateExpression = "set measures=list_append(measures, :measure)"
                expressionAttributeValues = {
                    ":measure":[{
                        't': t,
                        's': Decimal(measureLines[2].strip().split()[1])
                    }]
                }
                #Check if the init/resume time must be updated
                if updateTresume: #Start call
                    newAssign = int(resultsLines[1].strip().split()[1])
                    
                    updateExpression = updateExpression + ", tresume=:resume, assigned=:nassigned"
                    expressionAttributeValues.update( {':nassigned': int(newAssign), ':resume' : int(tinit)} )
                    if updateTinit:
                        updateExpression = updateExpression + ", tinit=:init"
                        expressionAttributeValues.update( {':init' : int(tinit)} )
                elif finished: #Finish call
                    updateExpression = updateExpression + ", finished=:finish, assigned=:nassigned, done=:ndone, dt=:dt"
                    expressionAttributeValues.update({':finish': int(1), ':nassigned': int(ndone), ':ndone' : int(ndone), ':dt': int(dt)})
                else: #Report call
                    newAssign = int(resultsLines[1].strip().split()[1])
                    updateExpression = updateExpression + ", assigned=:nassigned, done=:ndone, dt=:dt"
                    expressionAttributeValues.update({':nassigned': int(newAssign), ':ndone' : int(ndone), ':dt': int(dt)})

                response = table.update_item(
                        Key={
                            'id': str(ID),
                            'worker': int(iw)
                        },
                        UpdateExpression = updateExpression,
                        ExpressionAttributeValues = expressionAttributeValues,
                        ReturnValues="UPDATED_NEW"
                    )
                if response['ResponseMetadata']['HTTPStatusCode'] != 200 or 'Attributes' not in response:
                    return {
                        'statusCode': 400,
                        'body': "Error updating dynamo table: %s" % e.response["Error"]["Message"]
                    }
                
            except ClientError as e:
                print("Error: Unable to update items at dynamoDB table %s" % DYNAMOTABLE)
                print("       Error: %s" % e.response["Error"]["Message"])                
                return {
                    'statusCode': 400,
                    'body': "Error updating dynamo table: %s" % e.response["Error"]["Message"]
                }
            
            #If is a report, check if the execution requires more workers
            if iw == nworkers-1 and not updateTresume and not finished:
                ETA = int(resultsLines[2].strip().split()[1])
                if ETA > targetETA:
                    fact = float(ETA)/float(targetETA)-1.0
                    newWorkers = int(fact*nworkers)+1
                    #Create new workers
                    print("INFO: ETA (%d) is greater than target time (%d). Create new workers" % (ETA,targetETA))
                    createWorkers(ID, nworkers, newWorkers, table, reportTime, ndone)
                    
                    
    #flog = open("/tmp/serverlog.txt","r")
    #logtext = flog.read().strip('\n')
    return {
        'statusCode': 200,
        'body': "\n".join(resultsLines) #Return resuls output as body
        #'body': json.dumps({"response": lines, "log": logtext}) #Return resuls output as body
    }

def report(p,ID,iw,event,targetETA=1000000000000, nworkers = 0, reportTime=60):
    
    #Check for required parameters
    if "nIter" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing iterations done'
        }
        
    if "dt" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing elapsed time'
        }
        
    #Get parameters
    nIter = int(event["queryStringParameters"]["nIter"])
    dt    = int(event["queryStringParameters"]["dt"])
    
    #Report worker
    p.stdin.write(b'1 %d %d %d\n' % (iw,nIter,dt))
    p.stdin.flush()
    
    #Check output and return the response
    return checkOutput(ID,iw,p,ndone=nIter,dt=dt,targetETA=targetETA,nworkers=nworkers,reportTime=reportTime)
    
def start(p,ID,iw,event,telaps,tinit):
    
    #Check for required parameters
    if "dt" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing elapsed time'
        }
        
    #Get parameters
    dt = int(event["queryStringParameters"]["dt"])
    
    #Send start instruction
    p.stdin.write(b'2 %d %d\n' % (iw,dt))
    p.stdin.flush()
    
    #Check output and return the response
    tresume = telaps-dt
    if tresume < 0:
        tresume = 0
    if(tinit < 0):
        return checkOutput(ID,iw,p,dt=dt,updateTresume=True,updateTinit=True,tinit=tresume)
    else:
        return checkOutput(ID,iw,p,dt=dt,updateTresume=True,updateTinit=False,tinit=tresume)
    
def finish(p,ID,iw,event):
    
    #Check for required parameters
    if "nIter" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing iterations done'
        }
        
    if "dt" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing elapsed time'
        }
        
    #Get parameters
    nIter = int(event["queryStringParameters"]["nIter"])
    dt    = int(event["queryStringParameters"]["dt"])
    
    #Finish worker
    p.stdin.write(b'3 %d %d %d\n' % (iw,nIter,dt))
    p.stdin.flush()

    #Check output and return a response
    return checkOutput(ID,iw,p,ndone=nIter,dt=dt,finished=True)

def lambda_handler(event, context):
    
    #Get environment variables
    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])
    BUCKETIN = str(os.environ['BUCKETIN'])
    PREFIXIN = str(os.environ['PREFIXIN'])

    #Add execution permission to load balance program
    #st = os.stat(dirLBexe)
    #os.chmod(dirLBexe, st.st_mode | stat.S_IEXEC)
    
    # Check common parameters
    ############################

    if "path" not in event:
        return{
            'statusCode': 400,
            'body': 'missing execution ID: "path" not found'
        }

    if "pathParameters" not in event:
        return{
            'statusCode': 400,
            'body': 'missing execution ID: "pathParameters" not found'
        }

    if "id" not in event["pathParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing execution ID: "id" not found'
        }
    
    if "queryStringParameters" not in event:
        return{
            'statusCode': 400,
            'body': 'missing report parameters'
        }

    if "worker" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing worker number'
        }

    if "requestContext" not in event:
        return{
            'statusCode': 400,
            'body': 'missing request context'
        }

    if "requestTimeEpoch" not in event["requestContext"]:
        return{
            'statusCode': 400,
            'body': 'missing request time epoch'
        }
        

    # Get common parameters
    #########################
    
    #Get execution ID
    ID = str(event["pathParameters"]["id"])
    #Get worker index
    iw = int(event["queryStringParameters"]["worker"])
    #Get path
    path = str(event["path"])
    #Get request time epoch
    epoch = int(event["requestContext"]["requestTimeEpoch"])/1000 #Convert to seconds from ms

    # Read dynamo table
    #########################

    try:
        #Get dynamo client
        dynamodb = boto3.resource('dynamodb')
        #Get the table
        table = dynamodb.Table(DYNAMOTABLE)
        response = table.get_item(Key={'id': ID, 'worker': int(-1)})
        if "Item" not in response:
            return {
                'statusCode': 400,
                'body': "Dynamo table/item doens't exist"
            }
        globItem = response["Item"]
        #Check if init time has been set
        if globItem['tinit'] < 0:
            #Set epoch as initial time
            response = table.update_item(
                        Key={
                            'id': ID,
                            'worker': int(-1)
                        },
                        UpdateExpression="set tinit=:t",
                        ExpressionAttributeValues={
                            ':t': int(epoch)
                        },
                        ReturnValues="UPDATED_NEW"
                    )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200 or 'Attributes' not in response:
                return {
                    'statusCode': 400,
                    'body': "Error updating dynamo table 'tinit' (ID: %s)" % ID
                }
            globItem['tinit'] = epoch
        
        #Iterate until final worker
        nworkers = 0
        workers = []
        while True:
            response = table.get_item(Key={'id': ID, 'worker': int(nworkers)})
            if "Item" in response:
                nworkers = nworkers + 1
                workers.append(response["Item"])
            else:
                break
        
        if nworkers < 1:
            return {
                'statusCode': 400,
                'body': "Missing workers"
            }
            
        if iw >= nworkers:
            return{
                'statusCode': 400,
                'body': "Worker out of range"
            }
    except ClientError as e:
        print("Error: Unable to read dynamoDB table %s" % DYNAMOTABLE)
        print("       Error: %s" % e.response["Error"]["Message"])
        return {
            'statusCode': 400,
            'body': "DynamoDB read fail"
        }    

    #Load dynamo table and start the server
    telaps = max(epoch-int(globItem['tinit']),0)
    p = loadTable(globItem,workers,telaps)

    #Check the operation to perform
    if path.endswith("/start"):
        #Is a start petition
        return start(p,ID,iw,event,telaps,workers[iw]["tinit"])
    elif path.endswith("/report"):
        #Is a report
        #Check if last worker has sent at least, three reports
        if len(workers[nworkers-1]['measures']) > 4:
            #Then, we can add new workers 
            targetETA = int(globItem['dt'])      #Get target execution time
            targetETA = max(targetETA-telaps,0)           #Calculate residual desired execution time
            reportTime = max(int(globItem['done']/2),10)  #Get workers report time
            return report(p,ID,iw,event,targetETA,nworkers,reportTime)
        else:
            #Don't add new workers until the last one has been started
            return report(p,ID,iw,event)

    elif path.endswith("/finish"):
        #Is a finish petition
        finishResponse = finish(p,ID,iw,event)
        
        #Upload the balance report
        keyReport = str(os.environ['PREFIXRESULTS']) + "/" + ID + "/worker-" + str(iw) + "/balance.rep"
        worker = workers[iw]
        tinit = worker['tinit']
        data = "# %d %d %d \n" % (worker['worker'], worker['done'], tinit)
            
        for measure in worker['measures']:
            data += " %d %E \n" % (tinit+measure['t'],measure['s'])

        #Upload report
        try:
            BUCKETOUT = str(os.environ['BUCKETOUT'])
            s3_client = boto3.client('s3')
            s3_client.put_object(Body=data,Bucket=BUCKETOUT, Key=keyReport)
        except ClientError as e:
            print("Error: Unable to create balance report file: %s" % keyReport)
            print("       Error: %s" % e.response["Error"]["Message"])
            
        #Send response
        return finishResponse
    else:
        #unknown petition
        return {
            'statusCode': 400,
            'body': "unknown petition"
        }

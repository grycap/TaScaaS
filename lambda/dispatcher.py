import json
import os
import stat
import uuid
import botocore
import boto3
from boto3 import client
from botocore.exceptions import ClientError
from decimal import *

def register(epoch,event):
    
    if "queryStringParameters" not in event:
        return{
            'statusCode': 400,
            'body': 'missing query parameters'
        } 

    if event["queryStringParameters"] == None:
        return{
            'statusCode': 400,
            'body': 'missing query parameters'
        }

    if "secret" not in event["queryStringParameters"]:
    	return{
	        'statusCode': 401,
            'body': 'Unauthorized'
	    }

    #Get secret
    secret = str(os.environ['SECRET'])
    if secret != event["queryStringParameters"]["secret"]:
        return{
            'statusCode': 401,
            'body': 'Unauthorized'
        }


    if "slots" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing available slots number'
        }

    if "maxSlots" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing available "maxSlots" number'
        }

    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])

    slots = int(event["queryStringParameters"]["slots"])
    maxSlots = int(event["queryStringParameters"]["maxSlots"])
    #Generate a uuid for this worker
    uuidstr = str(uuid.uuid1())
    try:
        #Get dynamo client
        dynamodb = boto3.resource('dynamodb')
        #Get the table
        table = dynamodb.Table(DYNAMOTABLE)
        #Add this worker
        updateExpression = "set workersInfo=list_append(workersInfo, :worker), workersID=list_append(workersID, :name)"
        expressionAttributeValues = {
            ":worker":[{
                't': int(epoch),          #last connection
                'slots': int(slots),      #usable slots
                'maxSlots': int(maxSlots) #maximum slots
            }],
            ":name":[uuidstr]
        }
            
        response = table.update_item(
                Key={
                    'id': '___JOB_DISPATCHER_ENTRY___',
                    'worker': int(-1)
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
        
        SCALETIME = int(os.environ['SCALETIME'])
        return {
            'statusCode': 200,
            'body': '%s' % json.dumps({"id": uuidstr, "scaleTime": SCALETIME})
        }
            
    except ClientError as e:
        print("Error: Unable to update items at dynamoDB table %s" % DYNAMOTABLE)
        print("       Error: %s" % e.response["Error"]["Message"])                
        return {
            'statusCode': 400,
            'body': "Error updating dynamo table: %s" % e.response["Error"]["Message"]
        }        

def purge(indexList):
    
    if len(indexList) == 0:
        return{
            'statusCode': 200,
            'body': 'Done'
        }    
    try:
        #Construct update expression
        updateExpression = "remove"
        for i in indexList:
            if i != 0:
                updateExpression += ", workersID[%d], workersInfo[%d]" % (i,i)
            else:                
                updateExpression += " workersID[%d], workersInfo[%d]" % (i,i)
        
        DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])
        #Get dynamo client
        dynamodb = boto3.resource('dynamodb')
        #Get the table
        table = dynamodb.Table(DYNAMOTABLE)
        
        table.update_item(
                Key={
                        'id': '___JOB_DISPATCHER_ENTRY___',
                        'worker': int(-1)
                    },
                UpdateExpression=updateExpression,
                ReturnValues="NONE"
            )
        return{
            'statusCode': 200,
            'body': 'Done'
        }
    except ClientError as e:
        print("Error: Unable to remove workers at job dispatcher entry in dynamo talbe %s" % DYNAMOTABLE)
        print("       Error: %s" % e.response["Error"]["Message"])
        return {
            'statusCode': 400,
            'body': 'Unable to remove workers at job dispatcher entry in dynamo talbe %s' % DYNAMOTABLE
        } 
    
def disconnect(ID,tableEntry,event):

    try:
        index = []
        index.append(tableEntry["workersID"].index(ID))
        return purge(index)
    except:
        return{
            'statusCode': 400,
            'body': 'Node ID not found: %s' % ID
        }
        
def getJobs(ID,requiredCap,dispatched,scaling,event):
    
    #Get environment variables
    BUCKETIN = str(os.environ['BUCKETIN'])
    PREFIXIN = str(os.environ['PREFIXIN'])
    BUCKETOUT = str(os.environ['BUCKETOUT'])
    PREFIXOUT = str(os.environ['PREFIXOUT'])
    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])

    if "queryStringParameters" not in event:
        return{
            'statusCode': 400,
            'body': 'missing query parameters'
        }    
        
    if event["queryStringParameters"] is None:
        return{
            'statusCode': 400,
            'body': 'missing query parameters'
        }        
    
    if "slots" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing available slots number'
        }

    # Get requested number of jobs
    njobs = int(event["queryStringParameters"]["slots"])
    if njobs < 1:
        return{
            'statusCode': 400,
            'body': 'Invalid number of requested jobs'
        }    

    # Check for job files
    try:
        #Get and sort configuration files
        s3 = boto3.client('s3')
        ls = s3.list_objects_v2(Bucket=BUCKETOUT,Prefix=PREFIXOUT)
        if 'Contents' not in ls:
            return {
                'statusCode': 200,
                'body': '%s' % json.dumps({'requiredCap': str(requiredCap)})
            }            
        files = ls['Contents']
        if len(files) == 0:
            return {
                'statusCode': 200,
                'body': '%s' % json.dumps({'requiredCap': str(requiredCap)})
            }
        get_last_modified = lambda obj: obj['LastModified']
        sortedFiles = [obj['Key'] for obj in sorted(files, key=get_last_modified, reverse=True)]
        del files
        
        if len(sortedFiles) > njobs:
            sortedFiles = sortedFiles[0:njobs]
         
    except botocore.exceptions.ClientError as e:
        print("Error: Unable to retrieve job configuration files from %s/%s " % (BUCKETOUT,PREFIXOUT))
        print("       Error: %s" % e.response["Error"]["Message"])
        return{
            'statusCode': 400,
            'body': 'Error: Unable to retrieve job configuration files from %s/%s' % (BUCKETOUT,PREFIXOUT)
        }
            
    #Download each configuration file
    configs = []
    for file in sortedFiles:
        try:
            body = s3.get_object(Bucket=BUCKETOUT,Key=file)['Body'].read().decode('utf-8')
            config = body.split()
            if len(config) < 5:
                #Bad formated file, skip it
                continue
            #Extract information
            execID = config[0]
            dataKey = config[1]
            reportTime = config[2]
            workerID = config[3]
            nIter = config[4]
                
            #Create a persistent url
            inputDataURL = s3.generate_presigned_url('get_object',
                                                        Params={
                                                                'Bucket': BUCKETIN,
                                                                'Key': dataKey
                                                            },
                                                        ExpiresIn=1200)
    
            object = {}                                                            
            object["ID"] = execID
            object["reportTime"] = reportTime
            object["worker"] = workerID
            object["data-url"] = inputDataURL
            object["nIter"] = nIter
            configs.append(object)
                
            #Delete configuration file
            s3.delete_object(Bucket=BUCKETOUT,Key=file)
            
        except botocore.exceptions.ClientError as e:
            print("Error: Unable to retrieve and process job configuration file from %s/%s " % (BUCKETOUT,file))
            print("       Error: %s" % e.response["Error"]["Message"])
            return{
                'statusCode': 400,
                'body': 'Error: Unable to retrieve and process job configuration file from %s/%s' % (BUCKETOUT,file)
            }
        
    #Update dispatched jobs
    dispatched += len(sortedFiles)
        
    #Update dynamo table if we are not in scaling step
    if scaling < 1:
        try:
            #Get dynamo client
            dynamodb = boto3.resource('dynamodb')
            #Get the table
            table = dynamodb.Table(DYNAMOTABLE)
            table.update_item(
                                Key={
                                    'id': '___JOB_DISPATCHER_ENTRY___',
                                    'worker': int(-1)
                                },
                                UpdateExpression="set dispatched=:d",
                                ExpressionAttributeValues={
                                    ':d': int(dispatched)
                                },
                                ReturnValues="NONE"
                            )
        except ClientError as e:
            print("Error: Unable to update dispatched jobs at dynamo table %s" % DYNAMOTABLE)
            print("       Error: %s" % e.response["Error"]["Message"])
        
    response = {'requiredCap': str(requiredCap), 'configs': configs}
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }    

def update(nodeID,epoch,requiredCap,tableEntry,event):

    try:
        #Get node index
        index = tableEntry["workersID"].index(nodeID)
    except:
        return{
            'statusCode': 400,
            'body': 'Node ID not found: %s' % nodeID
        }
        
    slots = -1
    maxSlots = -1
    if "queryStringParameters" in event:
        if event["queryStringParameters"] != None:
            if "slots" in event["queryStringParameters"]:
                slots = int(event["queryStringParameters"]["slots"])
            if "maxSlots" in event["queryStringParameters"]:
                maxSlots = int(event["queryStringParameters"]["maxSlots"])
    
    #Get Dynamo table name            
    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])
    
    try:
        #Get dynamo client
        dynamodb = boto3.resource('dynamodb')
        #Get the table
        table = dynamodb.Table(DYNAMOTABLE)
        updateExpression="set workersInfo[%d].t=:t" % index
        expressionAttributeValues={
            ':t': int(epoch)
        }
        if slots >= 0:
            updateExpression = updateExpression + ", workersInfo[%d].slots=:slots" % index
            expressionAttributeValues.update({':slots' : int(slots)})
        if maxSlots > 0:
            updateExpression = updateExpression + ", workersInfo[%d].maxSlots=:maxSlots" % index
            expressionAttributeValues.update({':maxSlots' : int(maxSlots)})
        table.update_item(
                            Key={
                                'id': '___JOB_DISPATCHER_ENTRY___',
                                'worker': int(-1)
                            },
                            UpdateExpression=updateExpression,
                            ExpressionAttributeValues=expressionAttributeValues,
                            ReturnValues="NONE"
                        )
                        
        SCALETIME = int(os.environ['SCALETIME'])
        return{
            'statusCode': 200,
            'body': '%s' % json.dumps({"requiredCap": str(requiredCap), "scaleTime": SCALETIME, "epoch": epoch})
        }
    except ClientError as e:
        print("Error: Unable to update last connection time at dynamo table %s" % DYNAMOTABLE)
        print("       Error: %s" % e.response["Error"]["Message"])    
        return {
            'statusCode': 400,
            'body': 'Unable to update last connection time at dynamo table %s' % DYNAMOTABLE
        }
        
def lambda_handler(event, context):

    #Get environment variables
    BUCKETOUT = str(os.environ['BUCKETOUT'])
    PREFIXOUT = str(os.environ['PREFIXOUT'])
    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])
    SCALETIME = int(os.environ['SCALETIME'])
    
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
          'body': 'missing path parameters'
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

    #Get path
    path = str(event["path"])
    #Get request time epoch
    epoch = int(event["requestContext"]["requestTimeEpoch"])/1000 #Convert to seconds from ms

    #Get information from Dynamo table
    ####################################
    try:
        #Get dynamo client
        dynamodb = boto3.resource('dynamodb')
        #Get the table
        table = dynamodb.Table(DYNAMOTABLE)
        response = table.get_item(Key={'id': '___JOB_DISPATCHER_ENTRY___', 'worker': int(-1)})
        dispatched = 0
        if "Item" not in response:
            #Job dispatcher entry has not been created. Create it
            table.put_item(
                Item={
                    'id': '___JOB_DISPATCHER_ENTRY___',
                    'worker': int(-1),
                    'tinit': int(epoch),
                    'tresume': int(epoch),
                    'dispatched': int(0),           #Jobs dispatched
                    'requiredCap': Decimal(-1.0),   #Required slots capacity
                    'isScaling': int(0),            #Scaling
                    'workersID': [],                #Workers IDs
                    'workersInfo': []               #Worker nodes information
                }
            )
            response = table.get_item(Key={'id': '___JOB_DISPATCHER_ENTRY___', 'worker': int(-1)})
            if "Item" not in response:
                return {
                    'statusCode': 400,
                    'body': "Unable to create Dynamo item"
                }
            item = response["Item"]
            dispatched = int(item["dispatched"])
            scaling = int(item["isScaling"])
            requiredCap = item["requiredCap"]
            telaps = 0
        else:
            #Extract information from dynamo entry
            item = response["Item"]
            dispatched = int(item["dispatched"])
            scaling = int(item["isScaling"])
            requiredCap = item["requiredCap"]
            telaps = max(epoch-int(item['tresume']),0)
            if telaps > SCALETIME and scaling < 1:

                #Get the number of scheduled jobs
                s3 = boto3.client('s3')
                ls = s3.list_objects_v2(Bucket=BUCKETOUT,Prefix=PREFIXOUT)
                if 'Contents' not in ls:
                    remaining = 0
                else:
                    remaining = len(ls['Contents'])
                #Obtain the number of dispatched jobs per scale unit time
                scaleTimes = float(telaps)/float(SCALETIME)
                dispatchedPerScale = float(dispatched)/scaleTimes
                
                #Calculate the number of available slots and maximum slots
                availableSlots = 0
                maxSlots = 0
                toPurge = []
                for iworker in range(len(item["workersInfo"])):
                    worker = item["workersInfo"][iworker]
                    #Use only the slots on active workers
                    dt = epoch-int(worker['t'])
                    if dt < 2*SCALETIME:
                        availableSlots += worker["slots"]
                        maxSlots += worker["maxSlots"]

                    elif dt > 5*SCALETIME:
                        #Purge this worker
                        toPurge.append(iworker)
                
                #Purge workers
                purge(toPurge)

                #Remove workers from download item too
                if len(toPurge) > 0:
                    toPurge.sort(reverse=True)
                    for index in toPurge:
                        del item["workersInfo"][index]
                        del item["workersID"][index]
                
                if dispatched == 0:
                    requiredSlots = float(remaining)/2.0+1.0
                else:
                    if dispatchedPerScale < availableSlots:
                        #There are idle slots, remove it
                        requiredSlots = max(dispatchedPerScale,1.0)
                    else:
                        balanceFactor = float(remaining)/float(dispatched)
                        if balanceFactor > 0.10: #Allow a deviation of 10%
                            #Update the number of required slots
                            requiredSlots = max(1.0,float(availableSlots) * (1.0+balanceFactor))
                
                #Calculate total platform required capacity
                if maxSlots > 0:
                    requiredCap = Decimal(requiredSlots)/Decimal(maxSlots)
                else:
                    requiredCap = -1.0
                #Update the dynamo table
                scaling = 1
                dispatched = 0
                table.update_item(
                                    Key={
                                        'id': '___JOB_DISPATCHER_ENTRY___',
                                        'worker': int(-1)
                                    },
                                    UpdateExpression="set requiredCap=:requiredCap, dispatched=:dispatched, isScaling=:scaling",
                                    ExpressionAttributeValues={
                                        ':requiredCap': Decimal(requiredCap),
                                        ':dispatched': int(0),
                                        ':scaling': int(scaling)
                                    },
                                    ReturnValues="UPDATED_NEW"
                                )
            elif telaps > 2*SCALETIME and scaling > 0:
                scaling = 0
                table.update_item(
                                    Key={
                                        'id': '___JOB_DISPATCHER_ENTRY___',
                                        'worker': int(-1)
                                    },
                                    UpdateExpression="set tresume=:t, isScaling=:scaling",
                                    ExpressionAttributeValues={
                                        ':t': int(epoch),
                                        ':scaling': int(scaling)
                                    },
                                    ReturnValues="UPDATED_NEW"
                                )
                
    except ClientError as e:
        print("Error: Unable to read dynamoDB table %s" % DYNAMOTABLE)
        print("       Error: %s" % e.response["Error"]["Message"])
        return {
            'statusCode': 400,
            'body': "DynamoDB read fail"
        } 
        
    # Check the request type
    ##########################
      
    if path.endswith("/register"):
        return register(epoch,event)
    else:
        #Check provided worker ID
        if "id" not in event["pathParameters"]:
            return{
                'statusCode': 400,
                'body': 'missing worker ID: "id" not found at path parameters'
            }
        #Check if the provided ID is in the worker list
        wID = str(event["pathParameters"]["id"])
        if wID not in item["workersID"]:
            return{
              'statusCode': 401,
              'body': 'Unknown worker ID'
            }
        elif path.endswith("/disconnect"):
            return disconnect(wID,item,event)
        elif path.endswith("/jobs"):
            return getJobs(wID,requiredCap,dispatched,scaling,event)
        elif path.endswith("/update"):
            return update(wID,epoch,requiredCap,item,event)

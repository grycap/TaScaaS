# TaScaaS
# Copyright (C) 2020-2021 - GRyCAP - Universitat Politecnica de Valencia
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
import os
import botocore
import boto3
import uuid
import time as tsleep
from decimal import *

def lambda_handler(event, context):
    for record in event['Records']:

        #check if event type is "ObjectCreated"
        eventName = record['eventName']
        if eventName.find("ObjectCreated:") != 0:
            print("not ObjectCreated event")
            continue
        
        # Event and environment variables
        ####################################
        
        #Extract bucket and key
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print("Created '.config' object at:")
        print("    Bucket = " + bucket)
        print("    Key = " + key)

        #Extract filename and prefix from key
        prefix, filename = os.path.split(key)
        print("Filename: %s" % filename)
        print("  Prefix: %s" % prefix)


        BUCKETOUT = str(os.environ['BUCKETOUT'])
        PREFIXOUT = str(os.environ['PREFIXOUT'])
        INITWORKERS = int(os.environ['INITWORKERS'])
        MAXWORKERS = int(os.environ['MAXWORKERS'])
        BUCKETIN = str(os.environ['BUCKETIN'])
        PREFIXIN = str(os.environ['PREFIXIN'])
        DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])
        TIMEOUT = int(os.environ['TIMEOUT'])

        if bucket != BUCKETIN:
            print("Error: Unexpected input bucket")
            print("       expected: %s" % BUCKETIN)
            print("   event source: %s" % bucket)
            continue

        if prefix != PREFIXIN:
            print("Error: Unexpected input prefix")
            print("       expected: %s" % PREFIXIN)
            print("   event source: %s" % prefix)
            continue

        #Get configuration json
        try:
            s3_client = boto3.client('s3')
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            objText = obj['Body'].read().decode('utf-8')
            configuration = json.loads(objText)
        except botocore.exceptions.ClientError as e:
            print("Error: Unable to download and parse configuration file: %s" % key)
            print("       Error: %s" % e.response["Error"]["Message"])
            continue

        #Check if the configuration has all required fields
        if "iterations" not in configuration:
            print("Error: Field 'iterations' not found at provided configuration")
            return

        if "time" not in configuration:
            print("Error: Field 'time' not found at provided configuration")
            return

        if "inputFile" not in configuration:
            print("Error: Field 'inputFile' not found at provided configuration")
            return

        #Check optional parameters
        if "initWorkers" in configuration:
            INITWORKERS = int(configuration["initWorkers"])
            print("Initial workers set to %d" % INITWORKERS)
        
        #Check if required initial workers exceeds the maximum
        if INITWORKERS > MAXWORKERS:
            print("Initial workers value (%d) exceeds the maximum allowed (%d)." % (INITWORKERS,MAXWORKERS))
            print("%d workers will be launched instead" % MAXWORKERS)
            INITWORKERS = MAXWORKERS        
        
        nIter=int(configuration["iterations"])
        time=int(configuration["time"])
        inputFile=str(configuration["inputFile"])

        print("Iterations to done: %d" % nIter)
        print("Target execution time: %d" % time)
        print("Input file: %s" % inputFile)

        # Worker configuration parameters
        ####################################

        #Calculate iterations per worker
        iterPerWorker = int(nIter/INITWORKERS)
        iterRes = nIter % INITWORKERS
        
        #Create the corresponding key for data package
        dataKey = PREFIXIN + "/" + inputFile
        
        #Generate a uuid
        uuidstr = str(uuid.uuid1())
        
        #Generate execution identifier
        execID = uuidstr + "_" + filename
        
        #Report time
        if time <= 0:
            reportTime = 1000000000
            time = 1000000000
        else:
            reportTime = int(time/20) #Perform 20 reports
            if reportTime < 20:
                reportTime = 20
        
        # Check input data
        ####################
        try:
            response = s3_client.head_object(Bucket=bucket, Key=dataKey)
        except botocore.exceptions.ClientError as e:
            print("Error: Expected data file (%s) not found" % dataKey)
            print("       Error: %s" % e.response["Error"]["Message"])
            continue
        
        #Create the common part of the configuration file
        configCommon = execID + " " + dataKey + " " + str(reportTime) + " "

        
        # Create DynamoDB items
        #########################
        
        try:
            #Get dynamo client
            dynamodb = boto3.resource('dynamodb')
            #Get the table
            table = dynamodb.Table(DYNAMOTABLE)
            #Calculate ttl
            ttl = TIMEOUT + int(tsleep.time())
            #Put a first worker with index -1 to store global constants
            table.put_item(
                Item={
                    'id': execID,
                    'worker': int(-1),
                    'tinit': int(-1),
                    'tresume': int(-1),
                    'dt': int(time),            #Target time
                    'assigned': int(nIter),
                    'finished': int(0),
                    'done': int(2*reportTime),  #Threshold
                    'measures': [],
                    'ttl': int(ttl)
                }
            )
            #Create a item for each active worker
            for i in list(range(INITWORKERS)):
                table.put_item(
                   Item={
                        'id': execID,
                        'worker': int(i),
                        'tinit': int(-1),
                        'tresume': int(-1),
                        'dt': int(0),
                        'assigned': int(iterPerWorker),
                        'finished': int(0),
                        'done': int(0),
                        'measures': [

                        ],
                        'ttl': int(ttl)
                    }
                )
        except botocore.exceptions.ClientError as e:
            print("Error: Unable to create items at dynamoDB table %s" % DYNAMOTABLE)
            print("       Error: %s" % e.response["Error"]["Message"])
            continue
        
        #Create the common key for configuration files
        keyConfigCommon = PREFIXOUT + "/" + execID + "_"
        
        #Create the configuration files in the output bucket
        print("Create %d configuration files for execution %s" % (INITWORKERS,execID))
        for i in list(range(INITWORKERS)):
            workerConfig = configCommon + str(i)
            if i == 0:
                workerConfig += " " + str(iterPerWorker+iterRes)
            else:
                workerConfig += " " + str(iterPerWorker)
            keyConfig = keyConfigCommon + str(i)
            s3_client.put_object(Body=workerConfig,Bucket=BUCKETOUT, Key=keyConfig)

    return
 

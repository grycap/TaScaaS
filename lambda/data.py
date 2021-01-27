# IM - Infrastructure Manager
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
from boto3 import client
from botocore.exceptions import ClientError

def lambda_handler(event, context):

    #Get environment variables
    BUCKETOUT = str(os.environ['BUCKETOUT'])
    PREFIXRESULTS = str(os.environ['PREFIXRESULTS'])
    DYNAMOTABLE = str(os.environ['DYNAMOTABLE'])

    #Check if worker ID and number have been provided
    if "pathParameters" not in event:
        return{
            'statusCode': 400,
            'body': 'missing path parameters'
	    }

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

    if "wID" not in event["queryStringParameters"]:
        return{
            'statusCode': 400,
            'body': 'missing worker ID: "wID" not found at query string'
        }

    wID = str(event["queryStringParameters"]["wID"])

    if "jobID" not in event["pathParameters"]:
        return{
          'statusCode': 400,
          'body': 'missing job ID: "jobID" not found at path parameters'
        }

    if "worker" not in event["pathParameters"]:
        return{
          'statusCode': 400,
          'body': 'missing worker number: "worker" not found at path parameters'
	    }

    jobID = str(event["pathParameters"]["jobID"])
    workerNum = str(event["pathParameters"]["worker"])

    #Get information from Dynamo table
    ####################################
    try:
        #Get dynamo client
        dynamodb = boto3.resource('dynamodb')
        #Get the table
        table = dynamodb.Table(DYNAMOTABLE)
        response = table.get_item(Key={'id': '___JOB_DISPATCHER_ENTRY___', 'worker': int(-1)})
        if "Item" not in response:
            return{
              'statusCode': 401,
              'body': 'Unknown worker ID'
            }
        else:
            #Extract information from dynamo entry
            item = response["Item"]
    except ClientError as e:
        print("Error: Unable to read dynamoDB table %s" % DYNAMOTABLE)
        print("       Error: %s" % e.response["Error"]["Message"])
        return {
            'statusCode': 400,
            'body': "DynamoDB read fail"
        }

    #Check if the provided ID is in the worker list
    if wID not in item["workersID"]:
        return{
          'statusCode': 401,
          'body': 'Unknown worker ID'
        }

    #Generate a presigned url
    try:
        s3 = boto3.client('s3')
        key = '%s/%s/worker-%s/results.dat' % (PREFIXRESULTS,jobID,workerNum)
        body = str(s3.generate_presigned_url('put_object',Params={'Bucket': BUCKETOUT, 'Key': key}, ExpiresIn=900))
        return {
            'statusCode': 200,
            'body': body
        }
    except ClientError as e:
        print("Error: Unable to create presigned upload url to '%s/%s'" % (BUCKETOUT,key))
        print("       Error: %s" % e.response["Error"]["Message"])
        return {
            'statusCode': 400,
            'body': "Presigned url creation fail"
        }


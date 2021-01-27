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

#!/usr/bin/python

import requests
import json
import time
import sys
import subprocess
import math
import signal

nodeID = ""
requiredCap = -1

def scale(reqCap,s,maxS):
    s2 = min(int(math.ceil(float(reqCap)*float(maxS))),maxS)
    return max(s2,1)
    

def signal_handler(sig, frame):
    print("Sending disconnect petition...")
    disconnectUrl = url + "/node/%s/disconnect" % nodeID
    requests.get(disconnectUrl)
    print(disconnectUrl)
    print("Done!")
    sys.exit(0)

#Check arguments
if len(sys.argv) < 7:
    print("usage: %s api-url slots max-slots sleep-time worker-executable secret" % (sys.argv[0]))
    sys.exit()

#Register signal handler
signal.signal(signal.SIGINT, signal_handler)

#Process arguments
url=sys.argv[1]
slots=int(sys.argv[2])
maxSlots=int(sys.argv[3])
sleepTime=float(sys.argv[4])
workerExec=sys.argv[5]
secret=sys.argv[6]

freeSlots=slots

processes = []

#Try to register this node
registerUrl = url + "/node/register?slots=%d&maxSlots=%d&secret=%s" % (slots,maxSlots,secret)
response = requests.get(registerUrl)
if response.status_code == 200:
    responseJSON = response.json()
    if "id" not in responseJSON:
        print("Error: 'id' not found at register response")
        sys.exit()
    if "scaleTime" not in responseJSON:
        print("Error: 'scaleTime' not found at register response")
        sys.exit()
    
    nodeID=responseJSON["id"]
    scaleTime=(2.0/3.0)*int(responseJSON["scaleTime"])
    print("ID: %s" % nodeID)
    print("scaleTime: %f" % scaleTime)
    
else:
    print("Unable to register at the specified service")
    print(response.text)
    sys.exit()

#Save elapsed time since last update
elapsUpdate = 0

#Begin the working loop
while True:
    elapsUpdate += sleepTime

    # Check for finished jobs
    ###########################
    if len(processes) > 0:
        nprocesses = len(processes)
        i = 0
        while i < nprocesses:
            poll = processes[i].poll()
            if poll != None:
                #Finished!
                print("Job finished!")
                processes.pop(i)
                freeSlots = min(slots,freeSlots+1)
                i = i-1
                nprocesses = nprocesses - 1
            i = i+1    
    
    #Check if an update is required
    ################################
    if elapsUpdate >= scaleTime:
        print("Send update petition")
        elapsUpdate = 0
        #Send a ping to the server
        updateUrl = url + "/node/%s/update" % (nodeID)
        response = requests.get(updateUrl)
        if response.status_code != 200:
            print("Error received from server at update petition:")
            print(response.text)
            time.sleep(sleepTime)
            continue
        
        responseJSON = response.json()
        if "requiredCap" not in responseJSON:
            print("Error: Unexpected response from server at update petition:")
            print(response.text)
            time.sleep(sleepTime)
            continue

        if "epoch" not in responseJSON:
            print("Error: Unexpected response from server at update petition:")
            print(response.text)
            time.sleep(sleepTime)
            continue
        
        newRequiredCap = float(responseJSON["requiredCap"])
        epoch = int(responseJSON["epoch"])
        
        print("%d - Required capacity: %f" % (epoch,newRequiredCap))
        #Check if is required a reescale
        if newRequiredCap > -0.1 and newRequiredCap != requiredCap:
            #Update last required capacity
            requiredCap = newRequiredCap
            #Calculate required slots
            newSlots = scale(newRequiredCap,slots,maxSlots)
            print("%d - Capacity updated from %d to %d" % (epoch,slots,newSlots))
            if newSlots != slots:
                slots = newSlots
                #Update free slots
                newFreeSlots = slots-len(processes)
                print("%d - Free slots updated from %d to %d" % (epoch,freeSlots,newFreeSlots))
                freeSlots = newFreeSlots
                #Send updated slots
                updateUrl = url + "/node/%s/update?slots=%d" % (nodeID,slots)
                requests.get(updateUrl)
    
    #Require new jobs
    ##################
    if freeSlots > 0:
        petition = url + "/node/%s/jobs?slots=%d" % (nodeID,freeSlots)
        response = requests.get(petition)
        
        #Check if the petition has been processed successfuly
        if response.status_code != 200:
            print("Error received from server at jobs petition:")
            print(response.text)
            time.sleep(sleepTime)
            continue

        responseJSON = response.json()

        if "requiredCap" not in responseJSON:
            print("Error: Unexpected response from server when requiring jobs:")
            print(response.text)
            time.sleep(sleepTime)
            continue
        
        
        if "configs" not in responseJSON:
            #No new jobs
            time.sleep(sleepTime)
            continue
            
        configs = responseJSON["configs"]
        njobs = len(configs)
        if njobs > 0:
            #Start received jobs
            print("Starting %d jobs" % njobs)
            
            for job in configs:
                if "ID" not in job:
                    print("Corrupted job, 'ID' missing")
                    continue
                if "reportTime" not in job:
                    print("Corrupted job, 'reportTime' missing")
                    continue
                if "worker" not in job:
                    print("Corrupted job, 'worker' missing")
                    continue
                if "data-url" not in job:
                    print("Corrupted job, 'data-url' missing")
                    continue
                if "nIter" not in job:
                    print("Corrupted job, 'nIter' missing")
                    continue
                
                endpoint = url + "/lb/" + job["ID"]
                uploadUrlRequest = url + "/results/upload/%s/%d?wID=%s" % (job["ID"],int(job["worker"]),nodeID)
                processes.append(subprocess.Popen([workerExec,
                                                   job["data-url"],
                                                   endpoint,
                                                   job["ID"],
                                                   job["worker"],
                                                   job["reportTime"],
                                                   uploadUrlRequest
                                                   ]))
                freeSlots = max(freeSlots - 1,0)
                time.sleep(sleepTime)
            
        else:
            #Wait for new jobs
            time.sleep(sleepTime)
            continue
    else:
        #No free slots, wait for job finish
        time.sleep(sleepTime)

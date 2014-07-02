#!/usr/bin/python


# Standard Library
import json
import time
import logging
import socket
from threading import Thread, Lock

# Third Party

# Local


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventBroadcastReceiver:

    def __init__(self, host='localhost', port=0):
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host,port))
        self.socket.listen(5)
        if port == 0:
            port = self.socket.getsockname()[1]
        self.port = port
        self.statusLock = Lock()
        self.status = {
            "jobs": {}
        }
        self.stageMap = {}
        self.idMap = {}
        
    def run(self):

        self.listener = Thread(target=self._run)
        self.listener.start()

    def _run(self):

        conn, address = self.socket.accept()
        data = ""
        while True:
            data += conn.recv(1024)
            while True:
                newline = data.find('\n')
                if newline != -1:
                    self.statusLock.acquire()
                    try:
                        jsonData = data[:newline]
                        self.processEvent(json.loads(data[:newline]))
                    except Exception as e:
                        print "ERROR: %s" % e
                    finally:
                        data = data[newline+1:]
                        self.statusLock.release()
                else:
                    break

    def processEvent(self, event):
        eventType = event['Event']

        # TODO DEAL WITH FAILED TASKS!!!

        if eventType == "SparkListenerApplicationStart":

            self.status['appName'] = event["App Name"]
            self.status['started'] = event["Timestamp"]

        elif eventType == "SparkListenerJobStart":
            
            jobId = event['Job ID']
            stages = event["Stage IDs"]
            properties = event["Properties"]
            jobInfo = {
                "id": jobId,
                "numStages" : len(stages),
                "stagesWaiting": stages,
                "stagesInProgress" : [], 
                "stagesComplete" : [], 
                "stages": { 
                    stage : { 
                        "id": stage,
                        "numTasks" : 0,
                        "tasksInProgress" : [], 
                        "tasksComplete" : [], 
                        "tasks": {},
                        "complete": False
                    }
                for stage in stages},
                "complete": False,
                "failed": False,
                "properties": properties
            }

            for stage in stages:
                self.stageMap[stage] = jobId

            if "spark.jobGroup.id" in properties:
                self.idMap[properties["spark.jobGroup.id"]] = jobId
                jobInfo['handle'] = properties["spark.jobGroup.id"]
                
            self.status['jobs'][jobId] = jobInfo

            # Clean up old, complete jobs
            i = 0
            keys = self.status['jobs'].keys()
            for key in keys:

                if len(self.status['jobs']) <= 100:
                    break

                if self.status['jobs'][key]['complete']:
                    del self.status['jobs'][key]

        elif eventType == "SparkListenerStageSubmitted":

            info = event["Stage Info"]
            stageId = info["Stage ID"]
            jobId = self.stageMap[stageId]

            job = self.status['jobs'][jobId]
            job['stagesWaiting'].remove(stageId)
            job['stagesInProgress'].append(stageId)
            stage = job['stages'][stageId]
            stage['numTasks'] = info["Number of Tasks"]

        elif eventType == "SparkListenerTaskStart":

            info = event["Task Info"]
            taskId = info["Task ID"]
            stageId = event["Stage ID"]
            jobId = self.stageMap[stageId]

            stage = self.status['jobs'][jobId]['stages'][stageId]

            stage["tasksInProgress"].append(taskId)
            stage["tasks"][taskId] = {
                "id": taskId,
                "started": info["Launch Time"]
            }

        elif eventType == "SparkListenerTaskEnd":

            info = event["Task Info"]
            taskId = info["Task ID"]
            stageId = event["Stage ID"]
            jobId = self.stageMap[stageId]

            stage = self.status['jobs'][jobId]['stages'][stageId]                

            stage["tasksInProgress"].remove(taskId)
            stage["tasksComplete"].append(taskId) 
            stage["tasks"][taskId]['finished'] = info["Finish Time"]

            # TODO Handle event where task ends in failure

        elif eventType == "SparkListenerStageCompleted":

            info = event["Stage Info"]
            stageId = info["Stage ID"]
            jobId = self.stageMap[stageId]

            job = self.status['jobs'][jobId]
            job['stagesInProgress'].remove(stageId)
            job['stagesComplete'].append(stageId)
            stage = job['stages'][stageId]
            stage["complete"] = True

        elif eventType == "SparkListenerJobEnd":

            jobId = event['Job ID']
            job = self.status['jobs'][jobId]
            job["complete"] = True

            result = event['Job Result']
            if result['Result'] == 'JobFailed':
                job["failed"] = True

    def getStatus(self):
        status = {}
        self.statusLock.acquire()
        try:
            status = dict(self.status.items())
        except Exception as e:
            print e
        finally:
            self.statusLock.release()
        return status

    def getProgress(self, jobType=None):

        status = self.getStatus()
        if jobType:
            status['jobs'] = {
                key: value for key, value in status['jobs'].items() if value['properties']['spark.job.type'] == jobType
            }
        status['jobs'] = {
            jobId: self._processJobStatusToProgress(info) 
            for jobId, info in status['jobs'].items()
        }

        return status

    def getJobStatus(self, jobName):

        jobId = self.idMap.get(jobName, None)
        status = self.getStatus()
        jobStatus = status['jobs'].get(jobId, {})
        
        return jobStatus

    def _processJobStatusToProgress(self, status):

        if len(status) == 0:
            return {}
        
        stages = status['stages']
        properties = status['properties']
        totalStages = len(stages)
        completeStages = len([stage for stage in stages.values() if stage['complete']])

        if totalStages == 0:
            completeStages = 1
            totalStages = 1

        progress = {
            "name": properties.get("spark.job.name", ""),
            "type": properties.get("spark.job.type", ""),
            "complete": status['complete'],
            "failed": status['failed'],
            "totalStages": totalStages,
            "completeStages": completeStages,
            "stageProgress": float(completeStages)/float(totalStages),
        }

        if "handle" in status:
            progress['handle'] = status['handle']

        if len(status["stagesInProgress"]) > 0:

            currentStage = stages[status["stagesInProgress"][0]]
            totalTasks = currentStage['numTasks']
            completeTasks = len(currentStage['tasksComplete'])
            
            if totalTasks == 0:
                completeTasks = 1
                totalTasks = 1

            progress["currentStage"] = {
                "totalTasks": totalTasks,
                "completeTasks": completeTasks,
                "taskProgress": float(completeTasks)/float(totalTasks)
            }

        return progress

    def getJobProgress(self, jobName):

        status = self.getJobStatus(jobName)

        return self._processJobStatusToProgress(status)

    def getRunningCount(self):

        return len([job for job in self.getStatus()['jobs'].values() if not job['complete']])

    def close():
        # TODO actually close shit up
        pass

#!/usr/bin/python


# Standard Library
import inspect
import json
import logging
import os
import sys
import time
import traceback
import uuid
from collections import defaultdict
from Queue import Queue
from cStringIO import StringIO
from threading import Thread, Lock

# Third Party
import boto.s3
import mixingboard
import pip

# Local
from chassis.database import db_session
from chassis.models import JobHistory
from chassis.util import processFormattedTableDescription
from receiver import EventBroadcastReceiver


# import pyspark junk (REQUIRES SPARK TO BE ON SYSPATH)
from pyspark import SparkFiles
from pyshark import SharkConf
from sparkler import SparklerContext


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# defaults
WORKER_MEMORY = "3300m"
SCHEDULER_POOLS = [
    "verylow",
    "low",
    "default",
    "high",
    "veryhigh"
]


class JobRunner:
    """
    Manages and runs pyspark jobs
    """

    MAX_FILE_SIZE = 1024*1024*16 # don't write out results files larger than 16 MBs

    def __init__(self, account, master, uploadFolder, iamUsername,
                accessKeyId, accessKeySecret, conf={}):
        """
        Initializes a job runner

        Params:
            account: a user account for this job runner
            conf: allow you to set configuration variables on a spark context
        """

        self.account = account
        self.region = mixingboard.REGION
        self.master = master
        self.conf = conf
        self.uploadFolder = uploadFolder
        self.iamUsername = iamUsername
        self.accessKeyId = accessKeyId
        self.accessKeySecret = accessKeySecret

        self.handles = defaultdict(dict)
        self.handlesLock = Lock()

        self.createContext()

    # TODO when the runner is isolated, this needs to pass back the history over stdout
    def _makeHistory(self, accountId, userId, event, jobId=None, jobHandle=None, jobType="spark", data={}):
        """
        Save an entry to the job history record

        Params:
            account: an account
            user: a user
            event: an event type to save
            jobId: a job id
            data: a json serializable object
        Returns:
            a history event
        """

        jobHistory = JobHistory(account_id=accountId, user_id=userId, event=event, jobType=jobType,
                                jobId=jobId, jobHandle=jobHandle, data=data)
        db_session.add(jobHistory)
        db_session.commit()

        return jobHistory

    def _makeHandle(self):
        """
        Generates a unique query handle. Not completely resistant to
        collisions but it's nearly impossible so... close enough

        Returns:
            a unique handle
        """

        return "%s_%s" % (
            uuid.uuid4().bytes.encode("base64")[:21].translate(None, "/+"),
            int(time.time()*1000)
        )

    def runSQLSync(self, sql):
        """
        Run a sql query and wait for the result

        Params:
            sql: a sql query
        Returns:
            the result of running a job
        """

        result = [row.split('\t') for row in self.context.sql(sql)]

        return result


    def _parseSQL(self, sql):

        # if we have multiple queries, break them apart
        queries = []
        buff = ""
        skip = False
        parenDepth = 0
        quoteType = None

        for c in sql:

            buff += c

            # deal with escaping
            if c == "\\":
                skip = True
                continue

            if not skip:

                if quoteType is None:

                    if c == "(":
                        parenDepth += 1

                    elif c == ")" and parenDepth > 0:
                        parenDepth -= 1

                    elif c == "'" or c == '"':
                        quoteType = c

                    elif c == ";" and parenDepth == 0:
                        buff = buff.strip()
                        queries.append(buff[:-1])
                        buff = ""
                        continue

                elif quoteType == c:
                    quoteType = None

            skip = False

        buff = buff.strip()
        if len(buff) > 0:
            queries.append(buff)

        return queries


    def runSQLAsync(self, sql, options={}):
        """
        Run a job

        Params:
            sql: a sql query
        Returns:
            a handle for retrieveing future information about a job
        """

        handle = self._makeHandle()

        def sqlRunInner(sql, handle, options):

            self.handlesLock.acquire()
            self.handles[handle]['running'] = True
            self.handles[handle]['options'] = options
            self.handlesLock.release()

            result = None
            queryResult = None
            columns = None
            outputDir = '/tmp/%s/shark/%s' % (
                self.iamUsername,
                handle,
            )

            self._makeHistory(
                self.account,
                options.get('user'),
                'job_start',
                jobHandle=handle,
                data={ "jobName": options.get("jobName") },
                jobType="sql"
            )


            try:

                self.context.setJobGroup(handle, "SQL Query by Jerb")
                self.context.setLocalProperty("spark.job.name", options.get("jobName","Untitled"))
                self.context.setLocalProperty("spark.job.type", options.get("jobType","sql"))

                poolName = options.get("priority","default")
                pool = poolName if poolName in SCHEDULER_POOLS else "default"
                self.context.setLocalProperty("spark.scheduler.pool", options.get("priority","default"))

                queries = self._parseSQL(sql)

                self.context.setLocalProperty('spark.job.numqueries', str(len(queries)))

                for i, sql in enumerate(queries):

                    # if we're at the last query, save it's output
                    if i == len(queries)-1:

                        try:
                            # TODO use hiveContext stuff and saveAsTextFile
                            self.context.sql("""
                                CREATE TABLE `__%s`
                                ROW FORMAT DELIMITED
                                FIELDS TERMINATED BY '\t'
                                STORED AS TEXTFILE
                                LOCATION '%s'
                                AS %s
                            """ % (
                                handle,
                                outputDir,
                                sql
                            ))
                            columns, cached = processFormattedTableDescription([row.split('\t') for row in self.context.sql('DESC FORMATTED `__%s`' % handle)])
                            self.context.sql("ALTER TABLE `__%s` SET TBLPROPERTIES ('EXTERNAL'='TRUE')" % handle)
                            self.context.sql("DROP TABLE `__%s`" % handle)
                        except:
                            queryResult = [row.split('\t') for row in self.context.sql(sql)]

                    else:

                        self.context.sql(sql)

                result = {
                    "success": True,
                    "data": {
                        "outputDir": outputDir,
                        "message": "Query complete, use fetchN to retrieve results",
                        "handle": handle
                    },
                    "options": options
                }

                if queryResult:
                    result["data"]["result"] = queryResult

                if columns:
                    result["data"]["columns"] = columns

            except Exception as e:

                result = {
                    "success": False,
                    "data" : {
                        "error": str(e),
                        "trace": str(traceback.format_exc()).replace('\t','    ').split('\n')
                    },
                    "options": options
                }

            self._exportResults(handle, result)

            self._makeHistory(
                self.account,
                options.get('user'),
                'job_complete',
                jobHandle=handle,
                data={ "jobName": options.get("jobName") },
                jobType="sql"
            )

            self.handlesLock.acquire()
            self.handles[handle]['running'] = False
            self.handlesLock.release()

        t = Thread(target=sqlRunInner, args=(sql, handle, options))
        t.start()

        return handle


    def runJobSync(self, job, options={}):
        """
        Run a job

        Params:
            job: a dictionary containing job information
            options: a dictionary of options to pass to the job
        Returns:
            the result of running a job
        """

        requirements = self._fetchRequirements(job["jobPath"])
        for jobFile in job["jobFiles"]:
            requirements.extend(self._fetchRequirements(jobFile))
        # make our requirements unique
        requirements = list(set(requirements))
        self._installRequirements(requirements, job["name"])

        sys.path.append(job["jobFolder"])

        main = __import__(job["jobModule"])
        reload(main)

        for jobFile in job["jobFiles"]:
            # truncate the ".py" extension from the job file
            jobFileModuleName = jobFile[:jobFile.rfind(".")]
            jobFileModule = __import__(jobFileModuleName)
            reload(jobFileModule)

        self._addRequirements(job["jobPath"], job["jobFiles"], os.path.join(job["jobFolder"], "deps"))

        result = {}
        if len(inspect.getargspec(main.run)[0]) == 1:
            result = main.run(self.context)
        else:
            result = main.run(self.context, options=options)

        return result

    def _exportResults(self, handle, result):

        # TODO make region configurable
        s3Conn = boto.s3.connect_to_region(self.region, aws_access_key_id=self.accessKeyId, aws_secret_access_key=self.accessKeySecret)
        bucket = s3Conn.get_bucket('quarry-data-%s' % self.region, validate=False)
        resultsKey = "tmp/%s/spark/%s" % (
            self.iamUsername,
            handle
        )

        key = bucket.new_key(resultsKey)
        key.set_contents_from_string(json.dumps(result))

    def runJobAsync(self, job, options={}):
        """
        Run a job

        Params:
            job: a dictionary containing job information
            options: a dictionary of options to pass to the job
        Returns:
            a handle for retrieveing future information about a job
        """

        handle = self._makeHandle()

        def jobRunInner(job, options, handle):

            self.handlesLock.acquire()
            self.handles[handle]['running'] = True
            self.handles[handle]['options'] = options
            self.handlesLock.release()

            self._makeHistory(
                self.account,
                options.get('user'),
                'job_start',
                jobHandle=handle,
                data={ "jobName": options.get("jobName") },
                jobType=options.get("jobType","spark")
            )

            result = None
            try:

                requirements = self._fetchRequirements(job["jobPath"])
                for jobFile in job["jobFiles"]:
                    requirements.extend(self._fetchRequirements(jobFile))
                # make our requirements unique
                requirements = list(set(requirements))
                self._installRequirements(requirements, job["name"])

                sys.path.append(job["jobFolder"])

                main = __import__(job["jobModule"])
                reload(main)

                self._addRequirements(job["jobPath"], job["jobFiles"], os.path.join(job["jobFolder"], "deps"))

                self.context.setJobGroup(handle, "SQL Query by Jerb")

                result = {}
                self.context.setLocalProperty("spark.job.name", options.get("jobName","Untitled"))
                self.context.setLocalProperty("spark.job.type", options.get("jobType","python"))
                if len(inspect.getargspec(main.run)[0]) == 1:
                    result = main.run(self.context)
                else:
                    result = main.run(self.context, options=options)

                result = {
                    "success": True,
                    "data": result,
                    "options": options
                }

            except Exception as e:

                result = {
                    "success": False,
                    "data": {
                        "error": str(e),
                        "trace": str(traceback.format_exc()).replace('\t','    ').split('\n')
                    },
                    "options": {}
                }

            self._exportResults(handle, result)

            self._makeHistory(
                self.account,
                options.get('user'),
                'job_complete',
                jobHandle=handle,
                data={ "jobName": options.get("jobName") },
                jobType=options.get("jobType","spark")
            )

            self.handlesLock.acquire()
            self.handles[handle]['running'] = False
            self.handlesLock.release()

        t = Thread(target=jobRunInner, args=(job, options, handle))
        t.start()

        return handle


    def getProgress(self, jobType=None):
        """
        Get the current overall status for this spark context

        Returns:
            a dictionary with a whole bunch of status info
        """

        return self.receiver.getProgress(jobType=jobType)


    def getStatus(self):
        """
        Get the current overall status for this spark context

        Returns:
            a dictionary with a whole bunch of status info
        """

        return self.receiver.getStatus()


    def getHandleStatus(self, handle):
        """
        Get the current status of a spark job

        Params:
            handle: a job handle
        Returns:
            the current status for the job
        """

        # TODO lock this method

        status = {
            "status": self.receiver.getJobStatus(handle),
            "running": self.handles[handle]['running']
        }

        return status


    def cancelJobWithHandle(self, handle):
        """
        Cancel a spark job

        Params:
            handle: a job handle
        """

        # TODO lock this method

        return self.context.cancelJobGroup(handle)


    def getHandleProgress(self, handle):
        """
        Get the current progress of a spark job

        Params:
            handle: a job handle
        Returns:
            the current progress for the job
        """

        # TODO lock this method

        progress = {
            "progress": self.receiver.getJobProgress(handle),
            "running": self.handles[handle]['running']
        }

        return progress


    def getResults(self, handle):
        """
        Get the results of the spark job running under
        the given handle

        Params:
            handle: a job handle
        Returns:
            results of the spark job
        """

        # todo lock this method

        if 'result' not in self.handles[handle]:

            return False, None

        else:

            results = self.handles[handle]['result']
            options = self.handles[handle]['options']
            del self.handles[handle]
            return True, {
                "results": results,
                "options": options
            }

    def stop(self):
        """
        Stop and job the job runner child process
        """

        self.jobSend.send(None)
        self.proc.join()

    def kill(self):
        """
        Stop and job the job runner child process
        """

        self.context.stop()
        self.proc.join()


    def _fetchRequirements(self, path):

        """
        Parse the special requirements dictionary at the start of the job file

        Params:
            path: the path to the code for the job
        Returns:
            an array of parsed pip package names
        """

        codeFile = open(path)

        code = ""
        requirements = []
        for line in codeFile.readlines():
            try:
                code += line
                requirements = json.loads(code)
            except Exception as e:
                pass

        return requirements

    def _installRequirements(self, reqs, jobName):
        """
        Use pip to install requirements locally and download them
        so they can be shipped to remote workers

        Params:
            reqs: an array of pip package names
            jobName: the name of the job, this determines where files will be kept
        """

        if len(reqs) > 0:
            args = ['install','--download-cache=%s/_pip_cache/' % self.uploadFolder]
            install_args = list(args)
            install_args.extend(reqs)
            res = pip.main(initial_args = install_args)
            download_args = list(args)
            download_args.extend(["--download","%s/%s/%s/deps" % (self.uploadFolder, self.account, jobName)])
            download_args.extend(reqs)
            res = pip.main(initial_args = download_args)

    def _addRequirements(self, jobMainFile, jobFiles, folder):
        """
        Modify the environment of the spark context to add
        the current file as well as pip dependencies to it

        Params:
            jobFile: the path to the file for the job
            folder: the path to a folder containing zipped
                pip dependencies
        """

        for reqFile in os.listdir(folder):
            self.context.addPyFile(os.path.join(folder, reqFile))
        self.context.addPyFile(jobMainFile)
        for filename in jobFiles:
            self.context.addPyFile(filename)
        # FIXME test under conditions where job server is not on same node as worker
        self.context.environment["PYTHONPATH"] += ":".join(["./%s" % folder for folder in os.walk(SparkFiles.getRootDirectory()).next()[1]])

    def createContext(self):

        conf = SharkConf()
        if self.conf:
            for key, value in self.conf.items():
                conf.set(key, value)
        conf.setMaster(self.master)

        conf.set("spark.executor.memory", WORKER_MEMORY)
        conf.set("spark.files.overwrite", "true")

        self.receiver = EventBroadcastReceiver()
        self.receiver.run()

        conf.set("spark.eventBroadcast.enabled", "true")
        conf.set("spark.eventBroadcast.remotePort", self.receiver.port)

        conf.set("spark.scheduler.mode", "FAIR")
        # TODO add fair scheduling weighted pool support

        appName = uuid.uuid4().bytes.encode("base64")[:21].translate(None, "/+")
        conf.setAppName(appName)

        # set up scheduler pools
        fairSchedulerFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "conf", "fairscheduler.xml")
        conf.set("spark.scheduler.allocation.file", fairSchedulerFile)

        self.context = SparklerContext(conf=conf, accessKeyId=self.accessKeyId, accessKeySecret=self.accessKeySecret,
                                       iamUsername=self.iamUsername, bucket='quarry-data-%s' % self.region)

        # add pyspark home to the python path
        self.context.environment["PYTHONPATH"] = ":%s:" % os.path.join(os.environ["SPARK_HOME"], "python")

        # warm up the object store
        self.context.sql("SHOW TABLES")

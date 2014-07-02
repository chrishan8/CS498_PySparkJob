#!/usr/bin/python


# Standard Library
import argparse
import json
import time
import os
import sys
import logging
import socket
import traceback
import uuid
from collections import defaultdict
from threading import Thread, Lock

# Third Party
import mixingboard
import pip
from chassis.models import JobHistory
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename

# Local


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# parse args
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-l', '--local', action='store_true', help='Run spark in local mode')
argParser.add_argument('-p', '--port', type=int, default=1989, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default='127.0.0.1', help='Set the host')
argParser.add_argument('-u', '--iam-username', type=str, required=True, help='The IAM username for this account')
argParser.add_argument('--access-key-id', type=str, required=True, help='The access key id for this account')
argParser.add_argument('--access-key-secret', type=str, required=True, help='The access key secret for this account')
argParser.add_argument('-s', '--spark-home', type=str, required=True, help='The location of the local spark directory')
argParser.add_argument('-S', '--shark-home', type=str, required=True, help='The location of the local shark directory')
argParser.add_argument('-m', '--mysql-jar', type=str, required=True, help='The location of the mysql connector jar')
argParser.add_argument('-C', '--cluster', type=str, required=True, help='The cluster to add this instance to')
argParser.add_argument('-A', '--account', type=str, required=True, help='The account to add this instance to')
args, _ = argParser.parse_known_args()

# put args in sensible all caps variables
LOCAL = args.local
HOST = args.host
PORT = args.port
IAM_USERNAME = args.iam_username
ACCESS_KEY_ID = args.access_key_id
ACCESS_KEY_SECRET = args.access_key_secret
SPARK_HOME = os.path.abspath(args.spark_home)
SHARK_HOME = os.path.abspath(args.shark_home)
MYSQL_JAR = os.path.abspath(args.mysql_jar)
CLUSTER = args.cluster
ACCOUNT = args.account


# add spark/shark home to the os environ
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["SHARK_HOME"] = SHARK_HOME
os.environ["SPARK_CLASSPATH"] = MYSQL_JAR

# add the pyspark/pyshark directory to our python path
sys.path.append(os.path.join(SHARK_HOME, "python"))
sys.path.append(os.path.join(SPARK_HOME, "python"))
sys.path.append(os.path.join(SPARK_HOME, "python/lib/py4j-0.8.1-src.zip"))

# import the job runner
from lib.runner import JobRunner


# helpers for file uploads
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'jobs')
ALLOWED_EXTENSIONS = set(['py'])

def extension(filename):
    return filename.rsplit('.', 1)[1]

def allowed_file(filename):
    return '.' in filename and \
        extension(filename) in ALLOWED_EXTENSIONS


# instantiate flask and configure some shit
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

sys.path.append(UPLOAD_FOLDER)


# set up and instantiate our globals so we can use them from within flask routes
global jobs
global runner

jobs = defaultdict(dict)
runner = None


###
# Context Management Methods
###


def getSparkMasters():
    """
    Retrieve the spark masters.

    Returns:
        spark master uri
    """

    return mixingboard.getService("spark-master", account=ACCOUNT, cluster=CLUSTER)


def getSparkMaster():
    """
    Retrieve the master for a given
    user/account.

    Returns:
        spark master uri
    """

    return getSparkMasters()[0]

def createRunner(name, account, user, conf={}):
    """
    Creates a reusable spark runner

    Params:
        name: a name for the runner
        account: an account
        user: a user
        conf: a json encoded configuration dictionary
    """

    master = ""
    if LOCAL:
        master = "local"
    else:
        masterInfos = getSparkMasters()
        master = "spark://%s" % ','.join(["%s:%s" % (info['host'], info['port']) for info in masterInfos])
    return JobRunner(account=account, master=master, conf=conf, uploadFolder=UPLOAD_FOLDER, 
                    iamUsername=IAM_USERNAME, accessKeyId=ACCESS_KEY_ID, accessKeySecret=ACCESS_KEY_SECRET)


###
# Job Management Methods
###


@app.route('/spark/jobs/saved')
def list_saved_jobs():
    """
    Get a list of saved jobs

    GetParams:
        account: an account
        user: a user
    Returns:
        a list of saved jobs
    """
    global jobs

    account = request.form['account']
    user = request.form['user']

    return jsonify(jobs[account])


@app.route('/spark/jobs/upload', methods = ['POST'])
def upload_job():
    """
    Upload a new job

    PostParams:
        account: an account
        user: a user
        file: a python file containing a runnable job
        name: a name to assign to your job
    Returns:
        a message and a status code
    """
    global jobs

    account = request.form['account']
    user = request.form['user']

    uploadedFile = request.files['file']
    jobName = request.form['name']

    if uploadedFile and allowed_file(uploadedFile.filename):

        jobFolder = os.path.join(app.config['UPLOAD_FOLDER'], account, jobName)
        try:
            os.makedirs(os.path.join(jobFolder, "deps"))
        except OSError:
            pass

        savePath = os.path.join(jobFolder, "%s_main.py" % jobName)
        uploadedFile.save(savePath)
        jobDesc = {
            "name": jobName,
            "created": int(time.time()*1000),
            "lastRun": 0,
            "timesRan": 0,
            "jobFolder": jobFolder,
            "jobModule": jobName+"_main",
            "jobPath": savePath,
            "jobFiles": []
        }

        jobs[account][jobName] = jobDesc

        return jsonify({"success":True,"message":"Successfully created a job named '%s'" % jobName})

    else:

        return jsonify({"success":False,"message":"You must upload a .py file"}), 400


@app.route('/spark/jobs/<jobName>/upload', methods = ['POST'])
def upload_file_to_job(jobName):
    """
    Upload a file to an existing job

    PostParams:
        account: an account
        user: a user
        file: a python file containing a runnable job
    Returns:
        a message and a status code
    """
    global jobs

    account = request.form['account']
    user = request.form['user']

    job = jobs[account][jobName]

    for uploadedFile in request.files.values():
        if not allowed_file(uploadedFile.filename):
            return jsonify({"success":False,"message":"You must upload a .py file"}), 400

    for uploadedFile in request.files.values():

        filename = secure_filename(uploadedFile.filename)
        jobFolder = os.path.join(app.config['UPLOAD_FOLDER'], account, jobName)
        savePath = os.path.join(jobFolder, filename)
        uploadedFile.save(savePath)

        job["jobFiles"].append(savePath)

    return jsonify({"success":True,"message":"Successfully added %s file(s) to job '%s'" % (len(request.files), jobName)})


@app.route("/spark/job/async/status")
def async_job_status():
    """
    Get the status of an async job

    GetParams:
        account: an account
        user: a user
        handle: an async job handle
    Returns:
        a json-object represneting a job's status
    """
    global runner

    handle = request.args['handle']
    account = request.args['account']
    user = request.args['user']

    return jsonify(runner.getHandleStatus(handle))


@app.route("/spark/job/async/progress")
def async_job_progress():
    """
    Get the progress of an async job

    GetParams:
        account: an account
        user: a user
        handle: an async job handle
    Returns:
        a json-object represneting a job's progress
    """
    global runner 

    handle = request.args['handle']
    account = request.args['account']
    user = request.args['user']

    return jsonify(runner.getHandleProgress(handle))


@app.route("/spark/job/async/cancel", methods=["POST"])
def async_job_cancel():
    """
    Cancel an async job

    GetParams:
        account: an account
        user: a user
        handle: an async job handle
    Returns:
        a status code
    """
    global runner 

    handle = request.form['handle']
    account = request.form['account']
    user = request.form['user']

    runner.cancelJobWithHandle(handle)

    return jsonify({})


@app.route("/spark/job/<jobName>/run", methods=['POST'])
def run_job_async(jobName):
    """
    Asynchronously run a previously uploaded job

    RouteParams:
        jobName: the name of the job to run
    GetParams:
        account: an account
        user: a user
        options: options to provide to the job
    Returns:
        a job handle for getting status updates and results
    """
    global jobs
    global runner

    account = request.form['account']
    user = request.form['user']
    options = json.loads(request.form.get("options","{}"))
    options['user'] = user

    try:
        job = jobs[account][jobName]
    except KeyError:
        return jsonify({
            "error": "Job '%s' does not exist" % jobName
        })

    handle = runner.runJobAsync(job, options)

    return jsonify({
        "handle": handle,
        "history": {
            "jobHandle": handle,
            "accountId": account,
            "userId": user,
            "event": "job_start",
            "data": {
                "jobName": options.get("jobName", "Untitled")
            },
            "jobType": options.get("jobType", "spark")
        }
    })


@app.route("/spark/job/<jobName>/run/sync", methods=['POST'])
def run_job_sync(jobName):
    """
    Synchronously run a previously uploaded job

    RouteParams:
        jobName: the name of the job to run
    GetParams:
        account: an account
        user: a user
        options: options to provide to the job
    Returns:
        a json object containing the result of the job
    """
    global runner
    global jobs

    account = request.form['account']
    user = request.form['user']

    options = json.loads(request.form.get("options","{}"))

    try:
        job = jobs[account][jobName]
    except KeyError:
        return jsonify({
            "error": "Job '%s' does not exist" % jobName
        })

    result = runner.runJobSync(job, options)

    return jsonify({
        "result": result,
        "history": {
            "jobHandle": handle,
            "accountId": account,
            "userId": user,
            "event": "job_start",
            "data": {
                "jobName": options.get("jobName", "Untitled")
            }
        }
    })


@app.route("/spark/sql/run", methods=['POST'])
def run_sql_async():
    """
    Asynchronously run a sql query

    GetParams:
        account: an account
        user: a user
        sql: the sql query to run
    Returns:
        a job handle for getting status updates and results
    """
    global runner

    account = request.form['account']
    user = request.form['user']
    options = json.loads(request.form.get("options","{}"))
    options['user'] = user

    sql = request.form['sql']

    handle = runner.runSQLAsync(sql, options)

    return jsonify({
        "handle": handle
    })


@app.route("/spark/sql/run/sync", methods=['POST'])
def run_sql_sync():
    """
    Synchronously run a SQL query

    GetParams:
        account: an account
        user: a user
        sql: a sql query
    Returns:
        a json object containing the result of the job
    """
    global runner

    account = request.form['account']
    user = request.form['user']

    sql = request.form['sql']

    result = runner.runSQLSync(sql)

    return jsonify({
        "rows": result
    })


@app.route("/spark/status")
def status():
    """
    Get the overall status for the runner

    GetParams:
        account: an account
        user: a user
    Returns:
        a json object containing the overall status
    """
    global runner

    account = request.args['account']
    user = request.args['user']

    status = runner.getStatus()

    return jsonify({
        "status": status
    })


@app.route("/spark/progress")
def progress():
    """
    Get the overall progress for the runner

    GetParams:
        account: an account
        user: a user
    Returns:
        a json object containing the overall progress
    """
    global runner

    account = request.args['account']
    user = request.args['user']

    jobType = request.args.get('jobType', None)

    progress = runner.getProgress(jobType)

    return jsonify({
        "progress": progress
    })


if __name__ == "__main__":

    # NO DEBUG MODE ON PURPOSE. IT FUCKS THINGS UP.
    # JUST RESTART MANUALLY. YES IT'S A PAIN. JUST DEAL WITH IT

    runner = createRunner('default', ACCOUNT, None)
    mixingboard.exposeService('job-server', port=PORT, account=ACCOUNT, cluster=CLUSTER)
    app.run(host=HOST, port=PORT, threaded=True)

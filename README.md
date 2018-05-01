pyspark-job-server
==================

A simple job server for pyspark. It handles dependency management/deployment and job execution.

spark-submit --driver-memory 8G --jars "scripts/jars/aws-java-sdk-1.7.4.jar,scripts/jars/hadoop-aws-2.7.3.jar,scripts/jars/joda-time-2.9.3.jar" scripts/total-positive-test.py

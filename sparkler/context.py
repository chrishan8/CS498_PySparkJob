from pyshark import SharkContext
from pyspark.sql import HiveContext

class SparklerContext(SharkContext):

    def __init__(self, *args, **kwargs):

        self.iamUsername = kwargs['iamUsername']
        self.accessKeyId = kwargs['accessKeyId']
        self.accessKeySecret = kwargs['accessKeySecret']
        self.bucket = kwargs['bucket']

        del kwargs['iamUsername']
        del kwargs['accessKeyId']
        del kwargs['accessKeySecret']
        if "bucket" in kwargs:
            del kwargs['bucket']

        super(SparklerContext, self).__init__(*args, **kwargs)

        self.hiveContext = HiveContext(self)

    def hql(self, *args):
        return self.hiveContext.hql(*args)

    def cacheTable(self, *args):
        return self.hiveContext.cacheTable(*args)

    def uncacheTable(self, *args):
        return self.hiveContext.cacheTable(*args)

    def dataset(self, name, limit=None):

        if limit is None and self._conf.get("sparkler.test.rows"):
            limit =  self._conf.get("sparkler.test.rows")
            print "LIMIT: %s" % limit

        rddQuery = "SELECT * FROM %s" % name
        if limit:
            rddQuery += " LIMIT %s" % limit

        return self.hql(rddQuery)

    def getDatasets(self):
        return self.hql("SHOW TABLES").collect()

    @property
    def s3RawRootFolder(self):
        return "s3n://%s:%s@%s/user/%s/data/warehouse" % (
            self.accessKeyId,
            self.accessKeySecret,
            self.bucket,
            self.iamUsername
        )

    def rawDataset(self, name, minSplits=None):
        datasetLocation = "%s/%s/" % (
            self.s3RawRootFolder,
            name
        )
        return self.textFile(datasetLocation, minSplits)

    def getRawDatasets(self):
        pass

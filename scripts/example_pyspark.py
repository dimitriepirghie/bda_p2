#!/bin/python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("PreprocessInput")
sc = SparkContext(conf=conf)

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")
files = sc.wholeTextFiles('gs://bda-p2-ddc/pmc_oa/1000-01-01_2005-01-01/records*.xml.gz', use_unicode=True, minPartitions=256)


def bla(xmlPath):
    print 'Bla called'
    print xmlPath
    return None

if __name__ == '__main__':
    print 'Main'
    print(files)
    xmlFiles = files.map(lambda file: (file[0], bla(file[1].encode('utf-8'))))
    print(xmlFiles)

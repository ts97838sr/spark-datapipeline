from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName('WorkflowApp').getOrCreate()

def read_file(paramList,fileType):
    """
    Called with the Param List with options based on file type
    CSV : Default encoding utf-8,Header=True,InferSchema=True
    Parquet : Default configuration
    :return:
    """
    fileType=fileType.upper()
    if paramList[0]['Key'] == 'path':
        if fileType == "CSV" or fileType == "DAT":
            df = spark.read.load(paramList[0]['value'],format='csv',header=True,sep=paramList[1]['value'],inferSchema=True)
        elif fileType == "JSON":
            df = spark.read.load(paramList[0]['value'],format='json')
        elif fileType == "PARQUET":
            df = spark.read.load(paramList[0]['value'])
        else:
            print("File Extension provided is not yet supported")
            exit(11)
    return df


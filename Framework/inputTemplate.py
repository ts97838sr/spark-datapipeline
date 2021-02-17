from pyspark.sql import SparkSession
import pandas as pd
import ast

spark = SparkSession.builder.getOrCreate()

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
        elif fileType == "FIXED":
            try:
                paramList[1]['Key']== "ColumnSpecification"
            except Exception as e:
                print(e)
                exit(110)
            else:
                col_spec=list(ast.literal_eval(paramList[1]['value']))
                print(col_spec)

            try:
                paramList[2]['Key'] == "columnName"
            except Exception as e:
                print(e)
                print("Custom column names not provided")
            else:
                columnName = paramList[2]['value'].split(",")
                print(columnName)
                data=pd.read_fwf(paramList[0]['value'],colspecs=col_spec,header=None,names=columnName)
                df = spark.createDataFrame(data.astype(str))

        else:
            print("File Extension provided is not yet supported")
            exit(11)
    return df


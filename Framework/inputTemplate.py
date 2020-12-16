from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName('WorkflowApp').getOrCreate()

class ReadFile(object):
    """
    Class for defining various file reading options
    """
    def __init__(self,Name):
        self.Name = Name
        print(Name)

    def extn(self):
        filename,file_extension = os.path.splitext(self)
        print(f'Extension is {file_extension}')
        return file_extension
    def read_file(self):
        """
        Called with the Self parameter of input file path
        CSV : Default encoding utf-8,Header=True,InferSchema=True
        Parquet : Default configuration
        :return:
        """
        filename,file_extension = os.path.splitext(self)
        if file_extension == ".csv":
            df = spark.read.csv(self,encoding=None,header=True, inferSchema=True)
        elif file_extension == ".json":
            df = spark.read.json(self)
        elif file_extension == ".parquet":
            df = spark.read.load(self)
        else:
            print("File Extension provided is not yet supported")
        return df

    def read_file_custom_header(self,column_names,skip):
        filename,file_extension = os.path.splitext(self)
        if file_extension == ".xlsx":
            filename.pandas.read_excel(open(self,'rb'),skiprows=skip,names=column_names)
        elif file_extension == ".csv":
            filename.pandas.read_csv(open(self,'rb'),skiprows=skip,names=column_names)
        return filename

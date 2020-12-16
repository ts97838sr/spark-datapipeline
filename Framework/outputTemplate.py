from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('WorkflowApp').getOrCreate()

class WriteFile(object):
    """
    Class for defining various file writing options
    """
    def __init__(self,dfName,dfType,dfPath):
        self.dfName = dfName
        self.dfType = dfType
        self.dfPath = dfPath

    def write_file(self,dfName,dfType,dfPath):
        """
        This function is used write csv/json/parquet for the associated dataframe.
        CSV files are being writen in overwrite mode with Header.
        PARQUET files are being writen with default settings and partition driven by data.

        :param dfName: Input Dataframe name
        :param dfType: Type of output file (csv/json/parquet)
        :param dfPath: Target file Name(With Path)
        :return: Success status 0
        """

        if dfType == "csv":
            dfName.write.csv(dfPath,mode='overwrite', header=True)
        elif dfType == "json":
            dfName.write.json(dfPath)
        elif dfType == "parquet":
            dfName.write.parquet(dfPath)
        else:
            print("File Extension provided is not yet supported")
        return 0

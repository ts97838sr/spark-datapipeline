from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def write_file(paramList,dfType,dfName):
    """
    This function is used write csv/json/parquet for the associated dataframe.
    CSV files are being writen in overwrite mode with Header.
    PARQUET files are being writen with default settings and partition driven by data.
    Has support for repartitioning and coalesce or default behaviour
    :param paramList: Param List with options based on file type
    :param dfType: Type of output file (csv/json/parquet)
    :param dfName: Input Dataframe name

    :return: Success status 0
    """
    dfType=dfType.upper()
    if paramList[0]['Key'] == 'path':
        listargs=len(paramList)
        print(f'listargs {listargs}')
        if listargs>2:
            if paramList[2]['Key']=="repartition":
                if dfType == "CSV" or dfType=="DAT":
                    dfName.repartition(paramList[2]['value']).write.csv(paramList[0]['value'],mode='overwrite', header=paramList[1]['value'])
                elif dfType == "json":
                    dfName.repartition(paramList[2]['value']).write.json(paramList[0]['value'])
                elif dfType == "parquet":
                    dfName.repartition(paramList[2]['value']).write.parquet(paramList[0]['value'])
                else:
                    print("File Extension provided is not yet supported")
            else:
                if dfType == "CSV" or dfType=="DAT":
                    dfName.coalesce(paramList[2]['value']).write.csv(paramList[0]['value'],mode='overwrite', header=paramList[1]['value'])
                elif dfType == "json":
                    dfName.coalesce(paramList[2]['value']).write.json(paramList[0]['value'])
                elif dfType == "parquet":
                    dfName.coalesce(paramList[2]['value']).write.parquet(paramList[0]['value'])
                else:
                    print("File Extension provided is not yet supported")
        else:
            if dfType == "CSV" or dfType=="DAT":
                dfName.write.csv(paramList[0]['value'],mode='overwrite', header=paramList[1]['value'])
            elif dfType == "json":
                dfName.write.json(paramList[0]['value'])
            elif dfType == "parquet":
                dfName.write.parquet(paramList[0]['value'])
            else:
                print("File Extension provided is not yet supported")
    return 0

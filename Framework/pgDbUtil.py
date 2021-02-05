import configparser
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def dbConfigReader(configLoc,dbType):
    """
    :param configLoc: Location of database config file
    :param dbType: Config section to identify database type
    :return:
    """

    config = configparser.ConfigParser()
    config.read(configLoc)
    db_prop = config[dbType]
    return db_prop

def tableUnload(configLoc,dbType,queryOp):
    """
    :param db_prop: extracted property value for database connection
    :param db_url: db url with db name
    :param queryOp: select query to unload data
    :return: spark dataframe with query output
    """
    db_properties={}
    db_prop=dbConfigReader(configLoc,dbType)

    db_url = db_prop['url']
    db_properties['username']=db_prop['username']
    db_properties['password']=db_prop['password']
    db_properties['url']= db_prop['url']
    db_properties['driver']=db_prop['driver']

    df_select = spark.read.format('jdbc').options(url = db_url,dbtable=queryOp[0]['value'],properties=db_properties).load()

    return df_select

def tableLoad(configLoc,dbType,jdbcDF):
    """
    :param configLoc:
    :param dbType:
    :return:
    """
    db_properties={}
    db_prop=dbConfigReader(configLoc,dbType)

    db_url = db_prop['url']
    db_properties['username']=db_prop['username']
    db_properties['password']=db_prop['password']
    db_properties['url']= db_prop['url']
    db_properties['driver']=db_prop['driver']
    try:
        jdbcDF.write.format("jdbc").options(url = db_url,table=jdbcDF,mode='append',properties=db_properties).save()
    except Exception as e:
        print(e)
        exit(12)
    else:
        return 0


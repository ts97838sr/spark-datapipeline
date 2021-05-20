import json,sys,os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext

import inputTemplate
import outputTemplate
import logHandler
import pgDbUtil
import inputHandler

def readJson(wfJSON):
    """
    This function is used to orchestrate the execution of the process by parsing the workflow json and creating the
    workflow steps in sqlcontext
    :param file: workflow json
    :return: Successfully completed Task Project Name
    """

    TASK_PROJECT = wfJSON['TASK_PROJECT']
    WF_STATUS = wfJSON['WF_STATUS']
    if WF_STATUS == 'A':
        wfSteps = wfJSON['TASK_NAME']
        for steps in range(len(wfSteps)):
            stepVal = wfSteps[steps]
            logger.info(f'Current Step {stepVal}')
            if stepVal['TASK_CATEGORY'] == 'input':
                filePath = stepVal['TASK_VALUE']['PARAM']
                fileType = stepVal['TASK_VALUE']['TYPE']
                logger.info(f'Input File Path with Arguments :{filePath} and file type is {fileType}')
                try:
                    dF1 = inputTemplate.read_file(filePath,fileType)
                except Exception as e :
                    logger.error(e)
                    exit(1)
                else:
                    stepVal['TASK_NAME']=dF1.createOrReplaceTempView(stepVal['TASK_NAME'])
                    logger.info(f'Successfully registered :{filePath} as a Temp Table')
                    logger.info("===========================================================")
            elif stepVal['TASK_CATEGORY'] == 'DBRead':
                configLoc = stepVal['TASK_VALUE']['CONFIG_LOC']
                dbType = stepVal['TASK_VALUE']['TYPE']
                queryOp = stepVal['TASK_VALUE']['PARAM']
                logger.info(f'DataBase Type :{dbType} and config file is {configLoc}')
                logger.info(f"Query type and value : {queryOp[0]['Key']} -- {queryOp[0]['value']}")
                logger.info("===========================================================")
                try:
                    dF3 = pgDbUtil.tableUnload(configLoc,dbType,queryOp)
                except Exception as e :
                    logger.error(e)
                    exit (4)
                else:
                    stepVal['TASK_NAME'] = dF3.createOrReplaceTempView(stepVal['TASK_NAME'])
            elif stepVal['TASK_CATEGORY'] == 'DBWrite':
                configLoc = stepVal['TASK_VALUE']['CONFIG_LOC']
                dbType = stepVal['TASK_VALUE']['TYPE']
                queryOp = stepVal['TASK_VALUE']['PARAM']
                logger.info(f'DataBase Type :{dbType} and config file is {configLoc}')
                logger.info(f"Query type and value : {queryOp[0]['Key']} -- {queryOp[0]['value']}")
                logger.info("===========================================================")
                try:
                    RCValue = pgDbUtil.tableLoad(configLoc,dbType,queryOp[0]['value'])
                except Exception as e :
                    logger.error(e)
                    exit (4)
                else:
                    logger.info(f"Successfully Writen to table : {queryOp[0]['value']}")
            elif stepVal['TASK_CATEGORY'] == 'transform':
                transformOp = stepVal['TASK_VALUE']['PARAM']
                if transformOp[0]['Key'] == 'query':
                    transformOpSql=transformOp[0]['Value']
                    logger.info(f'Transformation : {transformOpSql}')
                    try:
                        logger.info(f'Dataframe Name : {stepVal["TASK_NAME"]}')
                        dF2=spark.sql(transformOpSql)
                    except Exception as e:
                        logger.error(f'SQL Execution Failed for {transformOp} with {e}')
                        exit(2)
                    else:
                        logger.info('Transformation Step Successful .Processing next step to register as Temp Table')
                    try:
                        logger.info(f"Registering Temp Table:  {stepVal['TASK_NAME']}")
                        if transformOp[1]['Key'] == 'Cache' and transformOp[1]['Value'] == 'False':
                            stepVal['TASK_NAME']=dF2.createOrReplaceTempView(stepVal['TASK_NAME'])
                        else:
                            stepVal['TASK_NAME']=dF2.createOrReplaceTempView(stepVal['TASK_NAME'])
                            dF2.cache()
                    except Exception as e:
                        logger.error(e)
                        exit(3)
                    else:
                        logger.info(f'Successfully Registered temp Table')
                        logger.info("===========================================================")
                else:
                    logger.info(f'Order of Param in Json is not correct. Please correct and resubmit the job')
                    exit(400)
            elif stepVal['TASK_CATEGORY'] == 'output':
                outfilePath = stepVal['TASK_VALUE']['PARAM']
                outputType = stepVal['TASK_VALUE']['TYPE']
                inputDS = stepVal['TASK_VALUE']['INPUT']
                logger.info(f'Output File Path :{outfilePath}')
                logger.info(f'Output FileType is {outputType} and dataframe to be writen is {inputDS}')
                logger.info(f'{outfilePath[0]["Key"]} : {outfilePath[0]["value"]} ')
                logger.info(f'{outfilePath[1]["Key"]} : {outfilePath[1]["value"]} ')
                try:
                    outputTemplate.write_file(outfilePath,outputType,dF2)
                except Exception as e:
                    logger.error(e)
                    exit(4)
                else:
                    logger.info(f'Output has been writen to {outfilePath}')
                    logger.info("===========================================================")
            else:
                logger.info("========== WORKING ON THIS FEATURE ==============")
                exit(404)

    return TASK_PROJECT

def main():
    """
    The main function is used to trigger the parsing of the input workflow json and make calls to function modules
    as defined in the JSON
    :return: WorkFlow status for TASK PROJECT
    """
    try:
        logger.info('Submitted Workflow JSON for processing. Please refer to log file for additional details')
        WF_NAME=readJson(wfJSON)
    except Exception as e:
        logger.error(f'Please Correct the Input JSON and resubmit. Detailed Error Message below:{e}')
    else:
        logger.info('Successfully Completed Workflow. Please refer to log file for additional details')

if __name__ == "__main__":
    sc = SparkContext.getOrCreate(SparkConf())
    spark = SQLContext(sc)
    try:
        if os.path.isfile(sys.argv[1]):
            fileExtension = os.path.splitext(sys.argv[1])[1]
            print(f'================ File Extension is {fileExtension} ==================')
            if fileExtension == ".json":
                with open(sys.argv[1]) as inputJson:
                    wfJSON = json.load(inputJson)
            elif fileExtension == ".yaml" or fileExtension == ".yml":
                wfJSON = json.loads(inputHandler.convertInputArgs(sys.argv[1]))
            else:
                print('===File extension provided not supported yet.Work in progress to introduce support for xml ===')
                exit(2)
        else:
            print('=========== Invalid input config file path/filename =============')
            exit(3)
    except Exception as e:
        print(f' ============= Invalid Json Error {e} ===========')
    else:
        logger = logHandler.MyLogger(wfJSON['LOG_LOCATION'])
        logger.info("Started parsing input workflow")
    main()

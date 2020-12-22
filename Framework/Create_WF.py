import datetime
import json
import logging
import sys

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext

import inputTemplate
import outputTemplate


def logHandler(wfJSON):
    """
    This function Creates the log handler by extracting the log path from workflow json
    :param wfJSON: Input Json
    :return: log handler context
    """
    curr_timestamp = datetime.datetime.now().strftime("%Y%m%d_%I%M%s")
    logname = wfJSON['LOG_LOCATION']+'_' +curr_timestamp +'.log'
    logger =logging.getLogger()
    logger = logging.getLogger("workFlowLog")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(logname)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def readJson(wfJSON):
    """
    This function is used to orchestrate the execution of the process by parsing the workflow json and creating the
    workflow steps in sqlcontext
    :param file: workflow json
    :return: Successfully completed Task Project Name
    """

    TASK_PROJECT= wfJSON['TASK_PROJECT']
    WF_STATUS= wfJSON['WF_STATUS']
    if WF_STATUS == 'A':
        wfSteps=wfJSON['TASK_NAME']
        for steps in range(len(wfSteps)):
            stepVal=wfSteps[steps]
            logger.info(f'Current Step {stepVal}')
            if stepVal['TASK_CATEGORY'] == 'input':
                filePath=stepVal['TASK_VALUE']['PARAM']
                fileType=stepVal['TASK_VALUE']['TYPE']
                logger.info(f'Input File Path with Arguments :{filePath} and file type is {fileType}')
                try:
                    dF1=inputTemplate.read_file(filePath,fileType)
                except Exception as e :
                    logger.error(e)
                    exit(1)
                else:
                    stepVal['TASK_NAME']=dF1.createOrReplaceTempView(stepVal['TASK_NAME'])
                    logger.info(f'Successfully registered :{filePath} as a Temp Table')
                    logger.info("===========================================================")
            elif stepVal['TASK_CATEGORY'] == 'transform':
                transformOp = stepVal['TASK_VALUE']['PARAM'][0]
                logger.info(f'Transformation : {transformOp}')
                try:
                    logger.info(f'Dataframe Name : {stepVal["TASK_NAME"]}')
                    dF2=spark.sql(transformOp)
                except Exception as e:
                    logger.error(f'SQL Execution Failed for {transformOp} with {e}')
                    exit(2)
                else:
                    logger.info('Transformation Step Successful .Processing next step to register as Temp Table')
                try:
                    logger.info(f"Registering Temp Table:  {stepVal['TASK_NAME']}")
                    stepVal['TASK_NAME']=dF2.createOrReplaceTempView(stepVal['TASK_NAME'])
                except Exception as e:
                    logger.error(e)
                    exit(3)
                else:

                    logger.info(f'Successfully Registered temp Table')
                    logger.info("===========================================================")
            elif stepVal['TASK_CATEGORY'] == 'output':
                outfilePath = stepVal['TASK_VALUE']['PARAM']
                outputType = stepVal['TASK_VALUE']['TYPE']
                inputDS = stepVal['TASK_VALUE']['INPUT']
                logger.info(f'Output File Path :{outfilePath}')
                logger.info(f'Output FileType is {outputType} and dataframe to be writen is {inputDS}')
                logger.info(f'{outfilePath[0]["Key"]} : {outfilePath[0]["value"]} ')
                logger.info(f'{outfilePath[1]["Key"]} : {outfilePath[1]["value"]} ')
                try:
                    outputTemplate.write_file(outfilePath,outputType,inputDS)
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
        logger.error(f'Please Correct the Input JSON and resubmit. Detailed Error Message below:'
              f'{e}')
    else:
        logger.info('Successfully Completed Workflow. Please refer to log file for additional details')

if __name__ == "__main__":
    sc = SparkContext.getOrCreate(conf=SparkConf())
    spark = SQLContext(sc)
    try:
        with open(sys.argv[1]) as inputJson:
            wfJSON = json.load(inputJson)
    except Exception as e:
        print(f'Invalid Json Error {e}')
    else:
        logger=logHandler(wfJSON)
        logger.info("Started parsing input workflow")
    main()

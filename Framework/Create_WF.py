import datetime
import json
import logging
import sys

import pyspark
from pyspark.sql import SQLContext

import inputTemplate
import outputTemplate


def logHandler(wfJSON):
    curr_timestamp = datetime.datetime.now().strftime("%Y%m%d_%I%M%s")
    logname = 'workFlowLog' +curr_timestamp +'.log'
    logger =logging.getLogger()
    logger = logging.getLogger("workFlowLog")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('/Users/tamalsarkar/IdeaProjects/config_param/log/' + logname)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


def readJson(wfJSON):
    """
    :param file:
    :return:
    """

    TASK_PROJECT= wfJSON['TASK_PROJECT']
    WF_STATUS= wfJSON['WF_STATUS']
    if WF_STATUS == 'A':
        wfSteps=wfJSON['TASK_NAME']
        for steps in range(len(wfSteps)):
            stepVal=wfSteps[steps]
            logger.info(f'Current Step {stepVal}')
            if stepVal['TASK_KEY'] == 'input':
                filePath=stepVal['TASK_VALUE']['PARAM'][-1][5:]
                logger.info(f'Input File Path :{filePath}')
                try:
                    dF1=inputTemplate.ReadFile.read_file(filePath)
                except Exception as e :
                    logger.error(e)
                    exit(1)
                else:
                    stepVal['TASK_NAME']=dF1.createOrReplaceTempView(stepVal['TASK_NAME'])
                    logger.info(f'Successfully registered :{filePath} as a Temp Table')
                    logger.info("===========================================================")
            elif stepVal['TASK_KEY'] == 'transform':
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
                    logger.info(f"Successfully Registered temp Table")
                    logger.info("===========================================================")
            elif stepVal['TASK_KEY'] == 'output':
                outfilePath = stepVal['TASK_VALUE']['PARAM'][-1][5:]
                logger.info(f'Output File Path :{outfilePath}')
                outputType = stepVal['TASK_VALUE']['TYPE']
                logger.info(f'Output FileType: {outputType}')
                inputDS = stepVal['TASK_VALUE']['INPUT']
                logger.info(f'File to be writen: {inputDS}')
                try:
                    outputTemplate.WriteFile.write_file(dF2,dF2,outputType,outfilePath)
                except Exception as e:
                    logger.error(e)
                    exit(4)
                else:
                    logger.info(f'Output has been writen to {outfilePath}')
                    logger.info("===========================================================")
            else:
                """default msg"""

    return TASK_PROJECT

def main():
    """
    The main function is used to trigger the parsing of the input workflow json and make calls to function modules
    as defined in the JSON
    :return: WorkFlow status for TASK PRJECT
    """
    try:
        print('Submitted Workflow JSON for processing. Please refer to log file for additional details')
        WF_NAME=readJson(wfJSON)
    except Exception as e:
        print(f'Please Correct the Input JSON and resubmit. Detailed Error Message below:'
              f'{e}')
    else:
        print('Successfully Completed Workflow. Please refer to log file for additional details')

if __name__ == "__main__":
    conf = pyspark.SparkConf()
    sc = pyspark.SparkContext.getOrCreate(conf=conf)
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

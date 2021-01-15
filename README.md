# spark-datapipeline
Data pipeline built on spark ,leveraging PySpark for frame-work and spark sql for processing.

Initial Rollout :

#SUPPORTED INPUT FORMAT:
CSV/PARQUET/JSON
CSV : Default encoding utf-8,Header=True,InferSchema=True
PARQUET : DEAFULT PARAMS

#SUPPORTED DB:
POSTGRES DB UNLOAD(FULL TABLE UNLOAD AS OF NOW ..)- WIP

#SUPPORTED TRANSFORMATION :
SPARK SQL

#SUPPORTED WRITE FORMAT :
CSV/PARQUET/JSON

CSV files are being writen in overwrite mode with Header.
PARQUET files are being writen with default settings and partition driven by data.

Complete Json Template available at
https://github.com/ts97838sr/spark-datapipeline/blob/dev-main/config_param/TEST_JSON_INPUT.json

OVERALL APPROACH:
USER INPUT is accepted in form of a json, where the user will provide the input file(s) path.

JSON Template
{
"TASK_NAME":"INPUT_DF",
"TASK_KEY":"FileRead",
"TASK_CATEGORY":"input",
"TASK_VALUE":{
"TYPE":"csv",
"PARAM":[
        {
        "Key":"path",
        "value":"Fully qualified input file path"
        },
        {
        "Key":"Separator",
        "value":","
        }
        ]
},
"START_TS":"CURRENT_TIMESTAMP",
"UPDATED_BY":"TAMAL"
},
Set of spark sql steps executed on the input files.The sql's will be directly provided in json and will be executed in the order in which received in the json file.

JSON Template

{
"TASK_NAME":"TABLE_DF",
"TASK_KEY":"TableUnload",
"TASK_CATEGORY":"DataBase",
"TASK_VALUE":{
"TYPE":"postgresql",
"CONFIG_LOC":"wfConfig.ini",
"PARAM":[
        {
        "Key":"query",
        "value":"users"
        }
        ]
    },
"START_TS":"CURRENT_TIMESTAMP",
"UPDATED_BY":"USER"

FULL TABLE UNLOAD

JSON Template

{
    "TASK_NAME":"TRANSFORM_DF1",
    "TASK_KEY":"transform",
    "TASK_VALUE":{
    "TYPE":"sql",
    "PARAM":[
    "SELECT firstname,lastname,address from INPUT_DF where id in (1,2,3)"
    ]
    },
    "START_TS":"CURRENT_TIMESTAMP",
    "UPDATED_BY":"USER"
}

The final step is writing in csv and parquet file using the dataframe to be writen and to a path specified.

JSON Template

{
"TASK_NAME":"OUTPUT_DF",
"TASK_CATEGORY":"output",
"TASK_KEY":"FileWrite",
"TASK_VALUE":{
"TYPE":"csv",
"INPUT":"TRANSFORM_DF2",
"PARAM":[
        {
        "Key":"path",
        "value":"Fully qualified output path"
        },
        {
        "Key":"header",
        "value":"True"
        }
        ]
},
"START_TS":"CURRENT_TIMESTAMP",
"UPDATED_BY":"TAMAL"
}


#Command line Trigger :

spark-submit --driver-class-path /JarDownload/postgresql-42.2.18.jar  <Path of main entry point>/Create_WF.py <Path of config>/config_param/TEST_JSON_INPUT.json

Jar to be downloaded and placed in a directory to which this needs to be pointed

##In case database feature is not being used in workflow, --driver-class-path is not required .

--driver-class-path /JarDownload/postgresql-42.2.18.jar
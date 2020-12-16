# spark-datapipeline
Data pipeline built on spark ,leveraging PySpark for frame-work and spark sql for processing.

Initial Rollout :

SUPPORTED INPUT FORMAT:
CSV/PARQUET/JSON
CSV : Default encoding utf-8,Header=True,InferSchema=True
PARQUET : DEAFULT PARAMS

SUPPORTED TRANSFORMATION :
SPARK SQL

SUPPORTED WRITE FORMAT :
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
    "TASK_KEY":"input",
    "TASK_VALUE":{
    "TYPE":"csv",
    "PARAM":[
    "path:<Fully qualified input file>"
    ]
    },
    "START_TS":"CURRENT_TIMESTAMP",
    "UPDATED_BY":"USER"
}
Set of spark sql steps executed on the input files.The sql's will be directly provided in json and will be executed in the order in which received in the json file.

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
    "TASK_KEY":"output",
    "TASK_VALUE":{
    "TYPE":"csv",
    "INPUT":"TRANSFORM_DF2",
    "PARAM":[
    "path:<Fully qualified output file>"
    ]
    },
    "START_TS":"CURRENT_TIMESTAMP",
    "UPDATED_BY":"USER"
}
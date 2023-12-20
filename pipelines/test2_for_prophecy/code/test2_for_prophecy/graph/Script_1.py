from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test2_for_prophecy.config.ConfigStore import *
from test2_for_prophecy.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    '''Script to filter seniorCitizen Male from others'''
    script_out_df = in0.filter(~ ((gender == 'Male') and (SeniorCitizen == 1))).select("*")

    return script_out_df

    return out0

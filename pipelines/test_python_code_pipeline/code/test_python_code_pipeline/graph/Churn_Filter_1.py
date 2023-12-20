from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test_python_code_pipeline.config.ConfigStore import *
from test_python_code_pipeline.udfs.UDFs import *

def Churn_Filter_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MonthlyCharge") >= lit(50)) & (col("DayMins") >= lit(100))))

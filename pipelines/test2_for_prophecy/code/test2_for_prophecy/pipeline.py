from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from test2_for_prophecy.config.ConfigStore import *
from test2_for_prophecy.udfs.UDFs import *
from prophecy.utils import *
from test2_for_prophecy.graph import *

def pipeline(spark: SparkSession) -> None:
    df_source_1 = source_1(spark)
    df_Script_1 = Script_1(spark, df_source_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test2_for_prophecy")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/test2_for_prophecy", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/test2_for_prophecy")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()

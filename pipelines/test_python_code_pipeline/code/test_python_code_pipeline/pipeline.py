from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from test_python_code_pipeline.config.ConfigStore import *
from test_python_code_pipeline.udfs.UDFs import *
from prophecy.utils import *
from test_python_code_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Churn_source_1 = Churn_source_1(spark)
    df_Churn_Filter_1 = Churn_Filter_1(spark, df_Churn_source_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test_python_code_pipeline")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/test_python_code_pipeline", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/test_python_code_pipeline")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()

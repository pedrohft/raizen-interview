import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext as sc
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
# from pyspark import SparkContext as sc

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

stg_fuel = (
        spark
        .read
        .format('csv')
        .option('header', True)
        .option('inferSchema', True)
        .option('delimiter', ',')
        .load("s3://raizen-pedro-teixeira/fuels/venda_combustiveis.csv")
)

stg_fuel = stg_fuel.withColumn("year_and_month",regexp_replace("year_and_month","/","_"))
(
    stg_fuel
    .write
    .mode('overwrite')
    .format('parquet')
    .partitionBy('year_and_month')
    .save('s3://raizen-pedro-teixeira/fuels_year_month/')
)


job.commit()
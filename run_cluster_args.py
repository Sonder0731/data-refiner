from pyspark import SparkConf
from pyspark.sql import SparkSession
from argparse import ArgumentParser
from data_refiner import Runner

parser = ArgumentParser()
parser.add_argument("--pipeline_name", type=str, required=True, help="Path to the config file")
args = parser.parse_args()

conf = SparkConf().set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark = SparkSession.builder.config(conf=conf).appName("data_refiner").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setCheckpointDir("hdfs:///checkpoints")
r = Runner.from_yaml(args.pipeline_name)
r.run(spark=spark)
spark.stop()

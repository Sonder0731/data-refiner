import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from argparse import ArgumentParser

from data_refiner.runner import Runner
from data_refiner.utils.path_set import LocalPath

parser = ArgumentParser()
parser.add_argument("--pipeline_file", type=str, required=True, help="Path to the config file")
args = parser.parse_args()

PYSPARK_PYTHON = str(
    LocalPath.repo_root().joinpath(
        ".venv/Scripts/python.exe" if sys.platform.startswith("win") else ".venv/bin/python"
    )
)

os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
conf = (
    SparkConf()
    .set("spark.pyspark.python", PYSPARK_PYTHON)
    .set("spark.pyspark.driver.python", PYSPARK_PYTHON)
    .set("spark.sql.execution.arrow.pyspark.enabled", "true")
)
spark = (
    SparkSession.builder.master("local[1]")
    .config(conf=conf)
    .appName("data_refiner")
    .enableHiveSupport()
    .getOrCreate()
)
sc = spark.sparkContext
sc.setCheckpointDir("./checkpoint")
r = Runner.from_yaml(args.pipeline_file)
r.run_locally(spark=spark)

import os
import sys
from pathlib import Path

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

from data_refiner.utils.path_set import LocalPath

TEST_ROOT = Path(__file__).parent.resolve()
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


@pytest.fixture(scope="session")
def spark(request):
    """
    创建一个 SparkSession，整个 pytest 会话共享
    """
    base_name = "data-refiner-runtime-resources"
    dependency_path = LocalPath.repo_root().joinpath(f"{base_name}.zip")
    graph_frames_path = LocalPath.runtime_resources_root().joinpath(
        "jars/graphframes-0.8.4-spark3.5-s_2.12.jar"
    )

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    conf = (
        SparkConf()
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.python.worker.memory", "1g")
        .set("spark.driver.memory", "1g")
        # .set("spark.storage.memoryFraction", "1")
        .set("spark.default.parallelism", "1")
        # .set("spark.sql.autoBroadcastJoinThreshold", "20485760")
        # .set("spark.sql.broadcastTimeout", "3600")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.ui.enabled", "false")
        .set("spark.jars", str(graph_frames_path.as_uri()))
    )
    spark = (
        SparkSession.builder.master("local[1]")
        .config(conf=conf)
        .appName("pytest-spark-session")
        .enableHiveSupport()
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setCheckpointDir("./checkpoint")
    is_local = request.config.getoption("--local")
    if not is_local:
        sc.addArchive(str(dependency_path.as_uri()) + f"#{base_name}")
    sc.addArchive(str(dependency_path.as_uri()) + f"#{base_name}")
    yield spark
    spark.stop()

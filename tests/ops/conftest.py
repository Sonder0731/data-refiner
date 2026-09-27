import os
import sys

import pytest
import shutil
from pyspark import SparkConf
from pyspark.sql import SparkSession

from data_refiner.ops_registry import register_ops as all_ops_register
from data_refiner.utils.path_set import LocalPath

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


@pytest.fixture
def regular_path_writer_path():
    path = LocalPath.test_operator_root() / ".pytest_tmp" / "regular_path_writer"
    shutil.rmtree(path, ignore_errors=True)
    yield str(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def register_ops():
    all_ops_register()


@pytest.fixture(scope="session")
def spark(request):
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
        .set("spark.default.parallelism", "1")
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
    # create a test database for testing table something
    test_db = "data_refiner_test_db_by_sonder0731"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    spark.sql(f"USE {test_db}")

    spark.sql("CREATE TABLE IF NOT EXISTS temp (id INT, name STRING)")
    spark.sql("INSERT INTO temp VALUES (1, 'Alice'), (2, 'Bob')")

    sc = spark.sparkContext
    sc.setCheckpointDir("./checkpoint")
    is_local = request.config.getoption("--local")
    if not is_local:
        sc.addArchive(str(dependency_path.as_uri()) + f"#{base_name}")
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {test_db} CASCADE")
    spark.stop()

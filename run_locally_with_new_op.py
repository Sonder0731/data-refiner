import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession

from data_refiner import Runner, registry, PathReader, resonance
from data_refiner.utils.path_set import LocalPath

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

"""
Define new processing operator
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType


class WholeTextFileReaderCopy(PathReader):
    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        spark = kwargs.get("spark")
        rdd = spark.sparkContext.wholeTextFiles(self.input_path)
        schema = StructType(
            [
                StructField("file_path", StringType(), True),
                StructField("text", StringType(), True),
            ]
        )
        return spark.createDataFrame(rdd, schema)


# registry operator
registry.register("whole_text_file_reader_copy", WholeTextFileReaderCopy)


r = Runner.from_dict(
    {
        "read_html_files": {
            "op_name": "whole_text_file_reader_copy",
            "input_path": "tests/pipeline/test_data/html",
            "output_df": "html_raw_df",
        },
        "extract_main_content": {
            "op_name": "html_content_extract_mapper",
            "input_df": "html_raw_df",
            "output_df": "html_content_df",
            "field": "text",
            "output_field": "content",
        },
        "chinese_identify": {
            "op_name": "language_identification_mapper",
            "input_df": "html_content_df",
            "output_df": "language_identification.df",
            "field": "content",
            "output_field": "language",
            "temp_view_name": "language_identification_temp_view",
        },
        "filter_chinese_documents": {
            "op_name": "spark_sql_executor",
            "output_df": "chinese_content_df",
            "sql_query": "SELECT file_path, content\nFROM language_identification_temp_view\nWHERE language =='zh'",
        },
        "jieba_tokenize": {
            "op_name": "jieba_chinese_tokenizer_mapper",
            "input_df": "chinese_content_df",
            "output_df": "tokenized_df",
            "field": "content",
            "output_field": "tokens",
        },
        "word_count": {
            "op_name": "array_flatmap_count_reducer",
            "input_df": "tokenized_df",
            "output_df": "word_count_df",
            "field": "tokens",
            "temp_view_name": "word_counts",
        },
        "filter_frequent_chinese_words": {
            "op_name": "spark_sql_executor",
            "input_df": "word_count_df",
            "output_df": "frequent_chinese_words_df",
            "table_name": "word_counts",
            "sql_query": "SELECT tokens AS word, count\nFROM word_counts\nWHERE count >= 2\n  AND tokens RLIKE '[\\\\u4e00-\\\\u9fff]'\nORDER BY count DESC, word ASC\n",
            "show": True,
        },
    }
)
r.run(spark=spark)

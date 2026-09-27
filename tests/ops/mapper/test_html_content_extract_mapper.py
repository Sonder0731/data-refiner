import os
from pathlib import Path

import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.html_content_extract_mapper import HtmlContentExtractMapper


@pytest.fixture(scope="class")
def records(spark):
    rdd = spark.sparkContext.wholeTextFiles(
        str(Path(os.environ["HTML_CONTENT_EXTRACT_MAPPER_TESTDATA_PATH"]).joinpath("*.html"))
    )
    df = spark.createDataFrame(rdd, ["path", "value"])
    df = df.unionByName(
        spark.createDataFrame(
            [("missing.html", None)],
            schema=df.schema,
        )
    )
    df.show()
    recorder.record("input_df", df)
    return recorder


class TestHtmlContentExtractMapper:
    def test_extract(self, records):
        mapper = HtmlContentExtractMapper(
            input_df="input_df",
            output_df="html_content_extract_mapper.df_tag",
            field="value",
            output_field="html_content",
            show=True,
            count=True,
            cache="disk",
        )
        df = mapper.process()
        assert df.collect()[0]["html_content"] != ""
        assert df.collect()[1]["html_content"] == None

import os
from pathlib import Path

import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.css_extraction_mapper import CssFeaturesExtractionMapper


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


@pytest.mark.usefixtures("spark", "records")
class TestCSSExtractionMapper:
    def test_extract(self, records):
        mapper = CssFeaturesExtractionMapper(
            input_df="input_df",
            output_df="css_extracted.df",
            field="value",
            output_field="css",
            show=True,
            count=True,
            cache="disk",
        )
        df = mapper.process()
        assert df.collect()[0]["css"][0] == "AsTdr"
        assert df.collect()[1]["css"] == None

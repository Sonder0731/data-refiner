import pytest

from data_refiner.core import recorder
from data_refiner.ops.builtin.sample import Sample


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        ("s", 1),
        ("s", 2),
        ("s", 3),
        ("s", 4),
        ("s", 5),
        ("s", 6),
        ("s", 7),
        ("s", 8),
        ("s", 9),
        ("s", 10),
    ]
    df = spark.createDataFrame(test_data, ["text", "index"])
    df.cache()
    recorder.record("test.df", df)
    return recorder


class TestSampleBuiltinOp:
    def test_sample_is_int(self, records):
        mapper = Sample(
            input_df="test.df",
            output_df="test.df_sample",
            sample=3,
            show=True,
        )
        df_processed = mapper.process()
        assert df_processed.count() == 3

    def test_sample_is_ratio(self, records):
        mapper = Sample(
            input_df="test.df",
            output_df="test.df_sample",
            sample=0.5,
            show=True,
        )
        df_processed = mapper.process()
        assert df_processed.count() == 5

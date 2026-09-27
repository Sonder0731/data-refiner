import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.edit_distance_mapper import EditDistanceMapper
from tests.tools import assert_same_by_dict


@pytest.fixture(scope="class")
def records(spark):
    test_data = [(1, "1234567", "012345678"), (2, None, "012345678")]
    df = spark.createDataFrame(test_data, ["__id__", "f_1", "f_2"])
    df.cache()
    recorder.record("shared_df", df)
    return recorder


class TestEditDistanceMapper:
    def test_edit_distance_mapper(self, spark, records):

        op = EditDistanceMapper(
            input_df="shared_df",
            output_df="edit_distance.df",
            fields=["f_1", "f_2"],
            output_field="edit_distance",
            select=["__id__", "edit_distance"],
            show=True,
        )
        df = op.process()
        assert_same_by_dict(
            df, [{"__id__": 1, "edit_distance": 2}, {"__id__": 2, "edit_distance": None}]
        )

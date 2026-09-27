import pytest
from pip._internal.commands import show

from data_refiner.core import recorder
from data_refiner.core.dependency import FilterLevel
from data_refiner.ops.filter.numeric_filter import NumericFilter
from tests.tools import assert_same_by_dict


@pytest.fixture(scope="class")
def records(spark):
    test_data_float = [
        (0.1,),
        (0.5,),
        (1.2,),
        (None,),
    ]

    test_data_int = [
        (1,),
        (10,),
        (100,),
        (None,),
    ]
    df_float = spark.createDataFrame(test_data_float, ["float_value"]).repartition(1)
    df_int = spark.createDataFrame(test_data_int, ["int_value"]).repartition(1)
    df_float.cache()
    df_int.cache()
    recorder.record("df_float", df_float)
    recorder.record("df_int", df_int)
    return recorder


class TestLengthFilter:
    def test_lt_float_with_tag(self, records):
        num_filter = NumericFilter(
            input_df="df_float",
            output_df="numeric_filter.df_tag",
            field="float_value",
            mode=FilterLevel.TAG,
            tag_field="tag",
            show=True,
            count=True,
            operator="lt",
            threshold=0.5,
        )
        df = num_filter.process()
        texts = [i for i in df.collect()]
        assert [i["tag"] for i in texts] == [True, False, False, True]

    def test_ge_float_with_tag(self, records):
        num_filter = NumericFilter(
            input_df="df_float",
            output_df="numeric_filter.df_tag",
            field="float_value",
            mode=FilterLevel.TAG,
            tag_field="tag",
            show=True,
            count=True,
            operator="ge",
            threshold=0.5,
        )
        df = num_filter.process()
        texts = [i for i in df.collect()]
        assert [i["tag"] for i in texts] == [False, True, True, True]

    def test_lt_int_with_tag(self, records):
        num_filter = NumericFilter(
            input_df="df_int",
            output_df="numeric_filter.df_tag",
            field="int_value",
            mode=FilterLevel.TAG,
            tag_field="tag",
            show=True,
            count=True,
            operator="lt",
            threshold=50,
        )
        df = num_filter.process()
        texts = [i for i in df.collect()]
        assert [i["tag"] for i in texts] == [True, True, False, True]

    def test_ge_int_with_tag(self, records):
        num_filter = NumericFilter(
            input_df="df_int",
            output_df="numeric_filter.df_tag",
            field="int_value",
            mode=FilterLevel.TAG,
            tag_field="tag",
            show=True,
            count=True,
            operator="ge",
            threshold=50,
        )
        df = num_filter.process()
        texts = [i for i in df.collect()]
        assert [i["tag"] for i in texts] == [False, False, True, True]

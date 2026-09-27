import pytest

from data_refiner.core import recorder
from data_refiner.core.dependency import FilterLevel
from data_refiner.ops.filter.length_filter import LengthFilter


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        ("AAAAAAAAAA",),
        ("AAAAAAAAAAAAAAAAAAAA",),
        ("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",),
        (None,),
    ]
    df = spark.createDataFrame(test_data, ["content"]).repartition(1)
    df.cache()
    recorder.record("shared_df", df)
    return recorder


class TestLengthFilter:
    def test_filter_with_tag(self, records):
        filter = LengthFilter(
            input_df="shared_df",
            output_df="length_filter.df_tag",
            field="content",
            min_length=15,
            max_length=25,
            mode=FilterLevel.TAG,
            tag_field="tag",
            show=True,
            count=True,
        )
        df = filter.process()
        texts = [i for i in df.collect()]
        assert [i["tag"] for i in texts] == [False, True, False, True]

    def test_filter_with_filter(self, records):
        filter = LengthFilter(
            input_df="shared_df",
            output_df="length_filter.df_filter",
            field="content",
            min_length=15,
            max_length=25,
            show=True,
        )
        df = filter.process()
        texts = [i["content"] for i in df.collect()]
        assert len(texts) == 2
        assert texts[0] == "AAAAAAAAAAAAAAAAAAAA"
        assert texts[1] == None

    def test_mode_with_tag_and_filter(self, records):
        filter = LengthFilter(
            input_df="shared_df",
            output_df="length_filter.df_tag_and_filter",
            field="content",
            min_length=15,
            max_length=25,
            tag_field="tag",
            show=True,
            mode=FilterLevel.TAG_AND_FILTER,
        )
        df = filter.process()
        texts = [i for i in df.collect()]
        assert len(texts) == 2
        assert texts[0]["content"] == "AAAAAAAAAAAAAAAAAAAA"
        assert texts[1]["content"] == None
        assert [i["tag"] for i in texts] == [True, True]

    def test_mode_with_tag_and_filter_with_only_max_length(self, records):
        filter = LengthFilter(
            input_df="shared_df",
            output_df="length_filter.df_tag_and_filter",
            field="content",
            max_length=15,
            tag_field="tag",
            show=True,
            mode=FilterLevel.TAG_AND_FILTER,
        )
        df = filter.process()
        texts = [i for i in df.collect()]
        assert len(texts) == 2
        assert texts[0]["content"] == "AAAAAAAAAA"
        assert texts[1]["content"] == None
        assert [i["tag"] for i in texts] == [True, True]

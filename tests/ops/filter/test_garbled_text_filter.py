import pytest

from data_refiner.core import recorder
from data_refiner.core.dependency import FilterLevel
from data_refiner.ops.filter.garbled_text_filter import GarbledTextFilter


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        ("2024 年 7 月 5 日 大连 劳动 公园 随拍",),
        ("² ³ º £ Ò ø Ð Ð ³ ¤ É ³ Í © è ÷ Æ Â Â · É ç Ç ø Ö § Ð Ð Õ ý Ê ½ ¿ ª Ò µ",),
        ("� � �� �� � � �� � ��É ³ Í © è ÷ Æ Â Â · � � � � � � �",),
        ("� � �� �� � � �� � ��大连 劳动 公园 随拍· � � � � � � �",),
        (None,),
    ]
    df = spark.createDataFrame(test_data, ["content"]).repartition(1)
    df.cache()
    recorder.record("shared_df", df)
    return recorder


@pytest.mark.usefixtures("spark", "records")
class TestGarbledTextFilter:
    def test_filter_with_tag(self, records):
        filter = GarbledTextFilter(
            input_df="shared_df",
            output_df="garbled_text_filter.df_tag",
            field="content",
            mode=FilterLevel.TAG,
            show=True,
            count=True,
        )
        df = filter.process()
        assert [row["is_garbled"] for row in df.collect()] == [False, True, True, True, False]

    def test_filter_with_filter(self, records):
        filter = GarbledTextFilter(
            input_df="shared_df",
            output_df="garbled_text_filter.df_tag",
            field="content",
            mode=FilterLevel.FILTER,
            show=True,
            count=True,
        )
        df = filter.process()
        assert df.collect()[0]["content"] == "2024 年 7 月 5 日 大连 劳动 公园 随拍"

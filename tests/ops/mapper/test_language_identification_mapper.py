import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.language_identification_mapper import LanguageIdentificationMapper
from tests.tools import assert_same_by_dict


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        (1, "你好呀，今天过得怎么样？"),
        (2, "Hello, how are you doing today?"),
        (3, "こんにちは、今日はどうですか？"),
        (4, "Bonjour, comment vas-tu aujourd'hui ?"),
        (5, "مرحبًا، كيف كان يومك اليوم؟"),
        (6, "Привет! Как прошел твой день?"),
        (7, "안녕하세요! 오늘 어떻게 지내셨어요?"),
        (8, None),
    ]
    df = spark.createDataFrame(test_data, ["__id__", "content"])
    df.cache()
    recorder.record("shared_df", df)


class TestLanguageIdentificationMapper:
    def test_mapper_with_label_and_prob(self, spark, records):
        mapper = LanguageIdentificationMapper(
            input_df="shared_df",
            output_df="fasttext_model_mapper.df_tag",
            field="content",
            output_field="language_label",
            show=True,
            count=True,
            drop=["content", "language_prob"],
        )
        df = mapper.process()
        assert_same_by_dict(
            df,
            [
                {"__id__": 1, "language_label": "zh"},
                {"__id__": 2, "language_label": "en"},
                {"__id__": 3, "language_label": "ja"},
                {"__id__": 4, "language_label": "fr"},
                {"__id__": 5, "language_label": "ar"},
                {"__id__": 6, "language_label": "ru"},
                {"__id__": 7, "language_label": "ko"},
                {"__id__": 8, "language_label": None},
            ],
        )

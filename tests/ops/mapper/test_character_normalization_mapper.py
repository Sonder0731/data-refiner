from data_refiner.core import recorder
from data_refiner.ops.mapper.character_normalization_mapper import CharacterNormalizationMapper

from tests.tools import assert_same_by_dict


class TestCharacterNormalizationMapper:
    def test_normalization_by_build_test_data(self, spark):
        test_data = [("1", "ℬ⼄⼆牛⼆㈡Ⅸ"), ("2", None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = CharacterNormalizationMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="normalized_text",
            show=True,
            select=["__id__", "normalized_text"],
        )
        df_processed = mapper.process(spark=spark)
        assert_same_by_dict(
            df_processed,
            [
                {"__id__": "1", "normalized_text": "B乙二牛二(二)IX"},
                {"__id__": "2", "normalized_text": None},
            ],
        )

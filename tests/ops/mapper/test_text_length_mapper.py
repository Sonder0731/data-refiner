from data_refiner.core import recorder
from data_refiner.ops.mapper.text_length_mapper import TextLengthMapper
from tests.tools import assert_same_by_dict


class TestTextLengthMapper:
    def test_text_length_mapper(self, spark):
        test_data = [(1, "ℬ⼄⼆牛⼆㈡Ⅸ"), (2, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = TextLengthMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="text_length",
            show=True,
            drop=["text"],
        )
        df_processed = mapper.process(spark=spark)
        assert_same_by_dict(
            df_processed, [{"__id__": 1, "text_length": 7}, {"__id__": 2, "text_length": None}]
        )

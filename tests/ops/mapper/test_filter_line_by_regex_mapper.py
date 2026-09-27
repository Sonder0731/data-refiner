from data_refiner.core import recorder
from data_refiner.ops.mapper.filter_line_by_regex_mapper import FilterLineByRegexMapper
from tests.tools import assert_same_by_dict


class TestFilterLineByRegexMapper:
    def test_function(self, spark):
        test_data = [(1, "2022-01-01\nhello world\n你好，世界！"), (2, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = FilterLineByRegexMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="text_output",
            regex="\d+",
            show=True,
            drop=["text"],
        )
        df_processed = mapper.process()
        assert_same_by_dict(
            df_processed,
            [
                {"__id__": 1, "text_output": "hello world\n你好，世界！"},
                {"__id__": 2, "text_output": None},
            ],
        )

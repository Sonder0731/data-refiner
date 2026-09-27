from data_refiner.core import recorder
from data_refiner.ops.mapper.chinese_traditional_to_simple_mapper import (
    ChineseTraditional2SimpleMapper,
)

from tests.tools import assert_same_by_dict


class TestChineseTraditional2SimpleMapper:
    def test_transform(self, spark):
        test_data = [("1", "義薄雲天"), ("2", None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = ChineseTraditional2SimpleMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="text_simple",
            drop=["text"],
            show=True,
        )
        df_processed = mapper.process()
        assert_same_by_dict(
            df_processed,
            [{"__id__": "1", "text_simple": "义薄云天"}, {"__id__": "2", "text_simple": None}],
        )

from data_refiner.core import recorder
from data_refiner.ops.mapper.duplicate_line_chr_fraction_mapper import (
    DuplicateLineChrFractionMapper,
)
from tests.tools import assert_same_by_dict


class TestDuplicateLineChrFractionMapper:
    def test_duplicate_line_chr_fraction(self, spark):
        test_data = [(1, "aaa\n\naaa\n\nbbb\naaa"), (2, ""), (3, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = DuplicateLineChrFractionMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="fraction",
            show=True,
            drop=["text"],
        )
        df_processed = mapper.process(spark=spark)
        assert_same_by_dict(
            df_processed,
            [
                {"__id__": 1, "fraction": 0.3529411852359772},
                {"__id__": 2, "fraction": 0.0},
                {"__id__": 3, "fraction": None},
            ],
        )

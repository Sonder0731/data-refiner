from data_refiner.core import recorder
from data_refiner.ops.mapper.duplicate_paragraph_chr_fraction_mapper import (
    DuplicateParagraphChrFractionMapper,
)
from tests.tools import assert_same_by_dict


class TestDuplicateParagraphChrFractionMapper:
    def test_duplicate_paragraph_chr_fraction(self, spark):
        test_data = [(1, "aaa\n\naaa\n\nbbb\naaa"), (2, ""), (3, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = DuplicateParagraphChrFractionMapper(
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
                {"__id__": 1, "fraction": 0.1764705926179886},
                {"__id__": 2, "fraction": 0.0},
                {"__id__": 3, "fraction": None},
            ],
        )

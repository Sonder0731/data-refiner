from data_refiner.core import recorder
from data_refiner.ops.mapper.duplicate_ngram_chr_fraction_mapper import (
    DuplicateNgramChrFractionMapper,
)
from tests.tools import assert_same_by_dict


class TestDuplicateNgramChrFractionMapper:
    def test_duplicate_ngram_chr_fraction(self, spark):
        test_data = [(1, "Please star this project. thanks thanks thanks!"), (2, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = DuplicateNgramChrFractionMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="fraction",
            show=True,
            ngram_range=(3, 4),
            drop=["text"],
        )
        df_processed = mapper.process(spark=spark)
        assert_same_by_dict(
            df_processed,
            [
                {
                    "__id__": 1,
                    "dup_3_ngram_chr_fraction": 0.5106382978723404,
                    "dup_4_ngram_chr_fraction": 0.44680851063829785,
                },
                {
                    "__id__": 2,
                    "dup_3_ngram_chr_fraction": None,
                    "dup_4_ngram_chr_fraction": None,
                },
            ],
        )

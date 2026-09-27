from data_refiner.core import recorder
from data_refiner.ops.mapper.top_ngram_chr_fraction_mapper import TopNgramChrFractionMapper
from tests.tools import assert_same_by_dict


class TestTopNgramChrFractionMapper:
    def test_top_ngram_chr_fraction(self, spark):
        test_data = [(1, "Please star this project. thanks thanks thanks!"), (2, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = TopNgramChrFractionMapper(
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
                    "top_3_gram_chr_fraction": 0.2553191489361702,
                    "top_4_gram_chr_fraction": 0.2553191489361702,
                },
                {
                    "__id__": 2,
                    "top_3_gram_chr_fraction": None,
                    "top_4_gram_chr_fraction": None,
                },
            ],
        )

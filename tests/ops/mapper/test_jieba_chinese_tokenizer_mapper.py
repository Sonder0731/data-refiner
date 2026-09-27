from data_refiner.core import recorder
from data_refiner.ops.mapper.jieba_chinese_tokenizer_mapper import JiebaNormalTokenizerMapper
from tests.tools import assert_same_by_dict


class TestJieBaNormalTokenizer:
    def test_tokenizer_punc_filter(self, spark):
        test_data = [(1, "你吃饭了吗?"), (2, None)]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)

        tokenizer = JiebaNormalTokenizerMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="tokens",
            show=True,
            drop=["text"],
        )
        df = tokenizer.process()
        assert_same_by_dict(
            df, [{"__id__": 1, "tokens": ["你", "吃饭", "了", "吗"]}, {"__id__": 2, "tokens": None}]
        )

    def test_tokenizer_no_punc_filter(self, spark):
        test_data = [(1, "你吃饭了吗?")]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)

        tokenizer = JiebaNormalTokenizerMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            punctuation_filter=False,
            output_field="tokens",
            show=True,
            drop=["text"],
        )
        df = tokenizer.process()
        assert_same_by_dict(df, [{"__id__": 1, "tokens": ["你", "吃饭", "了", "吗", "?"]}])

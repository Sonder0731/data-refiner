from data_refiner.core import recorder
from data_refiner.ops.mapper.nltk_tokenizer_mapper import NltkTokenizerMapper
from tests.tools import assert_same_by_dict


class TestNltkTokenizer:
    def test_normal_tokenizer(self, spark):
        test_data = [
            (
                1,
                "Don't forget to contact me if you have any questions. E-mail: qq82625757@gmail.com",
            ),
            (2, None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)

        tokenizer = NltkTokenizerMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="tokens",
            show=True,
            drop=["text"],
        )
        df = tokenizer.process()
        assert_same_by_dict(
            df,
            [
                {
                    "__id__": 1,
                    "tokens": [
                        'Do',
                        "n't",
                        'forget',
                        'to',
                        'contact',
                        'me',
                        'if',
                        'you',
                        'have',
                        'any',
                        'questions.',
                        'E-mail',
                        ':',
                        'qq82625757',
                        '@',
                        'gmail.com',
                    ],
                },
                {"__id__": 2, "tokens": None},
            ],
        )

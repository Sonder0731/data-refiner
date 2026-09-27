from data_refiner.core import recorder
from data_refiner.ops.mapper.unprintable_char_remove_mapper import UnprintableCharRemoveMapper
from tests.tools import assert_same_by_dict


class TestUnprintableCharRemoveMapper:
    def test_normal_tokenizer(self, spark):
        test_data = [
            (
                1,
                "If you wanna talk \tanything with me.\n Please email me at sonderbanana@gmail.com",
            ),
            (2, None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)

        tokenizer = UnprintableCharRemoveMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            exclude_chars="\n",
            output_field="sss",
            show=True,
            drop=["text"],
            renames={"sss": "cleaned_text"},
        )
        df = tokenizer.process()
        assert_same_by_dict(
            df,
            [
                {
                    "__id__": 1,
                    "cleaned_text": "If you wanna talk anything with me.\n Please email me at sonderbanana@gmail.com",
                },
                {"__id__": 2, "cleaned_text": None},
            ],
        )

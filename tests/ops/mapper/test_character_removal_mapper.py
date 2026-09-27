from data_refiner.core import recorder
from data_refiner.ops.mapper.character_removal_mapper import CharacterRemovalMapper
from tests.tools import assert_same_by_dict


class TestCharacterRemovalMapper:
    def test_character_removal(self, spark):
        test_data = [
            ("1", "Hello, World!"),
            ("2", "123-456-7890"),
            ("3", "Remove@#$Special%Chars&"),
            ("4", None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = CharacterRemovalMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="cleaned_text",
            characters_to_remove=",!@#$%&-",
            show=True,
        )
        df_processed = mapper.process(spark=spark)
        expected = [
            {"__id__": "1", "text": "Hello, World!", "cleaned_text": "Hello World"},
            {"__id__": "2", "text": "123-456-7890", "cleaned_text": "1234567890"},
            {
                "__id__": "3",
                "text": "Remove@#$Special%Chars&",
                "cleaned_text": "RemoveSpecialChars",
            },
            {"__id__": "4", "text": None, "cleaned_text": None},
        ]
        assert_same_by_dict(df_processed, expected)

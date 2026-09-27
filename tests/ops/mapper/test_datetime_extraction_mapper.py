from data_refiner.core import recorder
from data_refiner.ops.mapper.datetime_extraction_mapper import DatetimeExtractionMapper

from tests.tools import assert_same_by_dict


class TestDatetimeExtractionMapper:
    def test_datetime_extraction(self, spark):
        test_data = [
            (1, "Meeting scheduled for 2023-10-15 at 14:30"),
            (2, "The event will take place on December 25, 2024"),
            (3, "No date in this text"),
            (4, "Multiple dates: 2023-01-01 and 2024-02-14"),
            (5, None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = DatetimeExtractionMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            output_field="extracted_dates",
            show=True,
        )
        df_processed = mapper.process(spark=spark)
        expected = [
            {
                "__id__": 1,
                "text": "Meeting scheduled for 2023-10-15 at 14:30",
                "extracted_dates": ["2023-10-15 14:30:00"],
            },
            {
                "__id__": 2,
                "text": "The event will take place on December 25, 2024",
                "extracted_dates": ["2024-12-25 00:00:00"],
            },
            {"__id__": 3, "text": "No date in this text", "extracted_dates": []},
            {
                "__id__": 4,
                "text": "Multiple dates: 2023-01-01 and 2024-02-14",
                "extracted_dates": ["2023-01-01 00:00:00", "2024-02-14 00:00:00"],
            },
            {"__id__": 5, "text": None, "extracted_dates": None},
        ]
        assert_same_by_dict(df_processed, expected)

    def test_datetime_extraction_with_different_formats(self, spark):
        test_data = [
            (1, "15/10/2023 2:30 PM"),
            (
                2,
                "2023-10-15T14:30:00",
            ),
            (
                3,
                "Oct 15, 2023",
            ),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df2", df)
        mapper = DatetimeExtractionMapper(
            input_df="test.df2",
            output_df="test.df2_output",
            field="text",
            output_field="dates",
            show=False,
        )
        df_processed = mapper.process(spark=spark)
        expected = [
            {"__id__": 1, "text": "15/10/2023 2:30 PM", "dates": ["2023-10-15 14:30:00"]},
            {"__id__": 2, "text": "2023-10-15T14:30:00", "dates": ["2023-10-15 14:30:00"]},
            {"__id__": 3, "text": "Oct 15, 2023", "dates": ["2023-10-15 00:00:00"]},
        ]
        assert_same_by_dict(df_processed, expected)

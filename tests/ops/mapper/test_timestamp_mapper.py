from data_refiner.core import recorder
from data_refiner.ops.mapper.timestamp_mapper import TimestampMapper
from tests.tools import assert_same_by_dict


class TestTimestampMapper:
    def test_iso_format(self, spark):
        test_data = [(1, "2023-10-15T14:30:00"), (2, "2024-01-01 00:00:00"), (3, None)]
        df = spark.createDataFrame(test_data, ["__id__", "dt"])
        recorder.record("test.ts_df", df)
        mapper = TimestampMapper(
            input_df="test.ts_df",
            output_df="test.ts_df_out",
            field="dt",
            output_field="ts",
            show=True,
            tz="UTC+08:00",
            drop=["dt"],
        )
        df = mapper.process(spark=spark)
        assert_same_by_dict(
            df,
            [
                {"__id__": 1, "ts": 1697351400},
                {"__id__": 2, "ts": 1704038400},
                {"__id__": 3, "ts": None},
            ],
        )

    def test_various_formats(self, spark):
        test_data = [
            (1, "15/10/2023"),
            (2, "October 15, 2023"),
            (3, "2023-10-15"),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "dt"])
        recorder.record("test.ts_df2", df)
        mapper = TimestampMapper(
            input_df="test.ts_df2",
            output_df="test.ts_df2_out",
            field="dt",
            tz="UTC+8",
            output_field="ts",
            show=True,
            drop=["dt"],
        )
        df = mapper.process(spark=spark)
        assert_same_by_dict(
            df,
            [
                {"__id__": 1, "ts": 1697299200},
                {"__id__": 2, "ts": 1697299200},
                {"__id__": 3, "ts": 1697299200},
            ],
        )

    def test_invalid_string_returns_none(self, spark):
        test_data = [("not a date",)]
        df = spark.createDataFrame(test_data, ["dt"])
        recorder.record("test.ts_df3", df)
        mapper = TimestampMapper(
            input_df="test.ts_df3",
            output_df="test.ts_df3_out",
            field="dt",
            output_field="ts",
            show=False,
        )
        result = mapper.process(spark=spark)
        assert result.collect()[0]["ts"] is None

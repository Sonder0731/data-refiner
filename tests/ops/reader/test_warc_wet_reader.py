from data_refiner.ops.reader.warc_wet_reader import WarcWetReader
from data_refiner.utils.tools import get_env_var


class TestWarcWetReader:
    def test_read_response_file(self, spark):
        reader = WarcWetReader(
            input_path=get_env_var("PATH_READER_WARC_TESTDATA_PATH"),
            output_df="df_read_warc",
            types=["response"],
            show=True,
            count=True,
            cache="disk",
        )
        data = reader.process(spark=spark)
        assert data.count() == 1

    def test_read_conversion_file(self, spark):
        reader = WarcWetReader(
            input_path=get_env_var("PATH_READER_WET_TESTDATA_PATH"),
            output_df="df_read_warc",
            types=["conversion"],
            show=True,
            count=True,
            cache="disk",
        )
        data = reader.process(spark=spark)
        assert data.count() == 3

    def test_read_response_conversion_file(self, spark):
        reader = WarcWetReader(
            input_path=get_env_var("PATH_READER_CC_TESTDATA_PATH"),
            output_df="df_read_warc",
            types=["response", "conversion"],
            show=True,
            count=True,
            cache="disk",
        )
        data = reader.process(spark=spark)
        assert data.count() == 4

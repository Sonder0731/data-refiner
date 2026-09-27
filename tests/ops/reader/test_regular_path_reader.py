from data_refiner.ops.reader.regular_path_reader import RegularPathReader
from data_refiner.utils.tools import get_env_var


class TestPathReader:
    def test_read_json_file(self, spark):
        reader = RegularPathReader(
            input_path=get_env_var("PATH_READER_JSON_TESTDATA_PATH"),
            format="json",
            output_df="df_read_json",
            options={"recursiveFileLookup": True},
            show=True,
        )
        df = reader.process(spark=spark)
        assert [x.asDict() for x in df.collect()] == [
            {"text": "This is a test"},
            {"text": "B乙二牛二(二)IX"},
        ]

    def test_read_csv_file(self, spark):
        reader = RegularPathReader(
            input_path=get_env_var("PATH_READER_CSV_TESTDATA_PATH"),
            format="csv",
            output_df="df_read_csv",
            options={"multiLine": True},
            show=True,
        )
        df = reader.process(spark=spark)
        assert [x.asDict() for x in df.collect()] == [{"text": "good"}]

    def test_read_text_file(self, spark):
        reader = RegularPathReader(
            input_path=get_env_var("PATH_READER_CSV_TESTDATA_PATH"),
            format="text",
            output_df="df_read_text",
            show=True,
        )
        df = reader.process(spark=spark)
        assert [x.asDict() for x in df.collect()] == [{"value": "text"}, {"value": "good"}]

    def test_read_image_file(self, spark):
        reader = RegularPathReader(
            input_path=get_env_var("PATH_READER_IMAGE_TESTDATA_PATH"),
            format="image",
            output_df="df_read_image",
            show=True,
        )
        df = reader.process(spark=spark)
        assert df.count() == 3

    def test_read_audio_file(self, spark):
        reader = RegularPathReader(
            input_path=get_env_var("PATH_READER_AUDIO_TESTDATA_PATH"),
            format="binaryFile",
            output_df="df_read_audio",
            show=True,
        )
        df = reader.process(spark=spark)
        assert df.count() == 3

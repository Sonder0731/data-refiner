from data_refiner.ops.reader.whole_text_file_reader import WholeTextFileReader
from data_refiner.utils.tools import get_env_var


class TestWholeTextFileReader:
    def test_read_whole_text_file(self, spark):
        reader = WholeTextFileReader(
            input_path=get_env_var("HTML_CONTENT_EXTRACT_MAPPER_TESTDATA_PATH"),
            output_df="output.df",
            show=True,
            count=True,
            cache="disk",
        )
        data = reader.process(spark=spark)
        assert data.count() > 0

from data_refiner.ops.writer.path_writer import PathWriter
from data_refiner.utils.tools import get_env_var
from data_refiner.core import recorder


class TestPathWriter:
    def test_path_writer(self, spark, regular_path_writer_path):
        test_data = [
            ("1", "Hello, World!"),
            ("2", "123-456-7890"),
            ("3", "Remove@#$Special%Chars&"),
            ("4", None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)

        writer = PathWriter(
            input_df="test.df",
            format="json",
            mode="overwrite",
            path=regular_path_writer_path,
            show=True,
        )
        writer.process(spark=spark).show()

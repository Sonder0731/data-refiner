from data_refiner.ops.reader.hive_reader import HiveReader
from tests.tools import assert_same_by_dict


class TestTableReader:
    def test_read_hive_table(self, spark):
        reader = HiveReader(
            table_name="data_refiner_test_db_by_sonder0731.temp",
            output_df="df.read_table",
            show=True,
        )
        df = reader.process(spark=spark)
        assert_same_by_dict(
            df, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], id_column="id"
        )

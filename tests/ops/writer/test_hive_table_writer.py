import shutil

import pytest

from data_refiner.core.recorder import recorder
from data_refiner.ops.writer.hive_table_writer import HiveTableWriter


@pytest.fixture(scope="module")
def test_data(spark):
    """创建一个基础的测试 DataFrame"""
    data = [(1, "A", "2025-11-01"), (2, "B", "2025-11-02"), (3, "C", "2025-11-02")]
    recorder.record("test_input_data", spark.createDataFrame(data, ["id", "value", "dt"]))


@pytest.fixture(scope="function")
def cleanup_paths(tmp_path):
    """创建一个用于外部表数据的临时目录"""
    external_path = tmp_path / "external_data_location"
    yield str(external_path)
    if external_path.exists():
        shutil.rmtree(external_path)


class TestHiveTableWriter:
    def test_01_managed_table_create_and_append(self, spark, test_data):
        """测试托管表创建和数据的追加"""
        table_name = "data_refiner_test_db_by_sonder0731.managed_test_table"

        # overwrite
        writer_create = HiveTableWriter(
            input_df="test_input_data",
            output_df="x",
            table_name=table_name,
            save_mode="overwrite",
            partition_by=["dt"],
        )
        writer_create.process(spark=spark)

        assert spark.catalog.tableExists(table_name)
        assert spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0] == 3

        # Append
        data_append = [(4, "D", "2025-11-03")]
        df_append = spark.createDataFrame(data_append, recorder.load("test_input_data").schema)
        recorder.record("new_data", df_append)
        writer_append = HiveTableWriter(
            input_df="new_data",
            output_df="x",
            table_name=table_name,
            save_mode="append",
            partition_by=["dt"],
        )
        writer_append.process(spark=spark)

        final_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        assert final_count == 4

    def test_02_external_table_dynamic_overwrite(self, spark, test_data, cleanup_paths):
        """测试外部表创建和动态分区覆盖逻辑"""
        table_name = "data_refiner_test_db_by_sonder0731.external_test_table"

        writer_initial = HiveTableWriter(
            input_df="test_input_data",
            output_df="x",
            table_name=table_name,
            save_mode="overwrite",
            is_external=True,
            external_location=cleanup_paths,
            partition_by=["dt"],
            dynamic_partition_overwrite=False,
        )
        writer_initial.process(spark=spark)

        details = spark.catalog.getTable(table_name)
        assert details.tableType == "EXTERNAL"
        assert spark.sql(f"SELECT COUNT(DISTINCT dt) FROM {table_name}").collect()[0][0] == 2

        data_new = [(4, "D", "2025-11-02"), (5, "E", "2025-11-02")]
        df_new = spark.createDataFrame(data_new, recorder.load("test_input_data").schema)
        recorder.record("new_data", df_new)
        writer_dynamic = HiveTableWriter(
            input_df="new_data",
            output_df="x",
            table_name=table_name,
            save_mode="overwrite",
            is_external=True,
            external_location=cleanup_paths,
            partition_by=["dt"],
            dynamic_partition_overwrite=True,
        )
        writer_dynamic.process(spark=spark)

        final_data = spark.sql(f"SELECT id, dt FROM {table_name} ORDER BY id").collect()

        assert len(final_data) == 3
        assert any(row.id == 1 and row.dt == "2025-11-01" for row in final_data)
        assert not any(row.id in [2, 3] for row in final_data)
        assert all(row.id in [4, 5] for row in final_data if row.dt == "2025-11-02")

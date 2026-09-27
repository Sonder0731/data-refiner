from data_refiner.core import recorder
from data_refiner.ops.other.spark_sql_executor import SparkSqlExecutor


class TestSparkSqlExecutor:
    def test_basic_sql_query(self, spark):
        test_data = [
            (1, "Alice", 25, "Engineering"),
            (2, "Bob", 30, "Sales"),
            (3, "Charlie", 35, "Engineering"),
        ]
        df = spark.createDataFrame(test_data, ["id", "name", "age", "department"])
        df.createOrReplaceTempView("input_table")
        recorder.record("test.df", df)

        mapper = SparkSqlExecutor(
            output_df="test.df_output",
            sql_query="SELECT name, age, department FROM input_table WHERE department = 'Engineering'",
            show=True,
        )
        df_processed = mapper.process(spark=spark)

        # Assert results
        expected_data = [("Alice", 25, "Engineering"), ("Charlie", 35, "Engineering")]
        actual_data = df_processed.select("name", "age", "department").collect()

        assert len(actual_data) == len(expected_data)
        for actual_row, expected_row in zip(actual_data, expected_data):
            assert actual_row["name"] == expected_row[0]
            assert actual_row["age"] == expected_row[1]
            assert actual_row["department"] == expected_row[2]

    def test_sql_with_aggregation(self, spark):
        test_data = [
            (1, "Alice", 100, "Engineering"),
            (2, "Bob", 150, "Sales"),
            (3, "Charlie", 200, "Engineering"),
            (4, "David", 120, "Sales"),
        ]
        df = spark.createDataFrame(test_data, ["id", "name", "salary", "department"])
        df.createOrReplaceTempView("input_table_2")

        recorder.record("test.df2", df)

        mapper = SparkSqlExecutor(
            output_df="test.df2_output",
            sql_query="""
                SELECT department, 
                       COUNT(*) as employee_count,
                       AVG(salary) as avg_salary
                FROM input_table_2
                GROUP BY department
                ORDER BY department
            """,
            show=True,
        )
        df_processed = mapper.process(spark=spark)

        # Assert results
        expected_data = [("Engineering", 2, 150.0), ("Sales", 2, 135.0)]
        actual_data = df_processed.select("department", "employee_count", "avg_salary").collect()

        assert len(actual_data) == len(expected_data)
        for actual_row, expected_row in zip(actual_data, expected_data):
            assert actual_row["department"] == expected_row[0]
            assert actual_row["employee_count"] == expected_row[1]
            assert actual_row["avg_salary"] == expected_row[2]

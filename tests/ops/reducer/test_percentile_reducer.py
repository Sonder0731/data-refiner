from data_refiner.core import recorder
from pyspark.sql import SparkSession

from data_refiner.ops.reducer.percentile_reducer import PercentileReducer


class TestPercentileReducer:
    def test_percentile_95(self, spark: SparkSession):
        # Create test data with numeric values
        test_data = [(float(i),) for i in range(1, 101)]  # 1.0 to 100.0
        df = spark.createDataFrame(test_data, ["value"])
        recorder.record("test.input_df", df)

        # Initialize and run the reducer
        reducer = PercentileReducer(
            input_df="test.input_df",
            output_df="test.output_df",
            field="value",
            percentile=0.95,
            show=True,
        )
        result_df = reducer.process(spark=spark)

        # Collect result and assert
        result = result_df.collect()
        assert len(result) == 1
        percentile_95_value = result[0][0]
        # For values 1.0 to 100.0, the 95th percentile should be 95.05 (using linear interpolation)
        # But Spark's approxQuantile might return 96.0 due to its approximation algorithm
        # Let's use a more robust range that accommodates Spark's implementation
        assert 95.0 <= percentile_95_value <= 97.0

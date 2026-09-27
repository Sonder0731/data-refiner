from collections import Counter

import pytest

from data_refiner.core import recorder
from data_refiner.ops.sampler.stratified_sampler import StratifiedSampler


@pytest.mark.usefixtures("spark")
class TestFieldCountReducer:
    def test_transform(self, spark):
        test_data = [
            ("user1", "Alice", 25, "F", "Beijing"),
            ("user2", "Bob", 30, "M", "Beijing"),
            ("user3", "Charlie", 35, "M", "Shanghai"),
            ("user4", "Diana", 28, "F", "Shanghai"),
            ("user5", "Eve", 22, "F", "Shanghai"),
        ]
        schema = ["user_id", "name", "age", "gender", "city"]
        df = spark.createDataFrame(test_data, schema)
        recorder.record("test.df", df)
        mapper = StratifiedSampler(
            input_df="test.df",
            output_df="test.df_output",
            strata_column="city",
            samples_per_stratum=1,
            show=True,
        )
        df_processed = mapper.process()
        data = df_processed.collect()
        cities = [row["city"] for row in data]
        counts = Counter(cities)
        assert counts["Beijing"] == 1
        assert counts["Shanghai"] == 1

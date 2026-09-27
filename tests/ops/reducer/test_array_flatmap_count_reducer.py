import pytest

from data_refiner.core import recorder
from data_refiner.ops.reducer.array_flatmap_count_reducer import ArrayFlatmapCountReducer


@pytest.mark.usefixtures("spark")
class TestArrayFlatmapCountReducer:
    def test_reducer(self, spark):
        test_data = [(1, ["cat", "cat"]), (2, ["dog", "monkey"]), (3, ["cat"])]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)
        mapper = ArrayFlatmapCountReducer(
            input_df="test.df", output_df="test.df_output", field="text", show=True
        )
        df_processed = mapper.process()
        assert [x.asDict() for x in df_processed.collect()] == [
            {"text": "cat", "count": 3},
            {"text": "dog", "count": 1},
            {"text": "monkey", "count": 1},
        ]

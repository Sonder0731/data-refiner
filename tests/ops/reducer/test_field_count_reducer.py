import pytest

from data_refiner.core import recorder
from data_refiner.ops.reducer.field_count_reducer import FieldCountReducer


@pytest.mark.usefixtures("spark")
class TestFieldCountReducer:
    def test_transform(self, spark):
        test_data = [("cat",), ("dog",), ("cat",)]
        df = spark.createDataFrame(test_data, ["text"])
        recorder.record("test.df", df)
        mapper = FieldCountReducer(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
        )
        df_processed = mapper.process()
        assert [x.asDict() for x in df_processed.collect()] == [
            {"text": "cat", "count": 2},
            {"text": "dog", "count": 1},
        ]

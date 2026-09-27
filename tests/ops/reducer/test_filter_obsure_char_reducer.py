import pytest

from data_refiner.core import recorder
from data_refiner.ops.reducer.filter_obscure_char_reducer import FilterObscureCharReducer


@pytest.mark.usefixtures("spark")
class TestFilterObsureCharReducer:
    def test_reducer(self, spark):
        test_data = [("ABCABCABCABCABCABC",), ("ABCABCABCABCABCABC",), ("XYZXYZ",)]
        df = spark.createDataFrame(test_data, ["text"]).repartition(3)
        recorder.record("test.df", df)
        mapper = FilterObscureCharReducer(
            input_df="test.df",
            output_df="test.df_output",
            field="text",
            cumsum_rate=0.9,
            show=True,
        )
        df_processed = mapper.process()
        assert [x.asDict() for x in df_processed.collect()] == [
            {
                "char": "X",
                "frequency": 2,
                "cumsum": 38,
                "cumsum_rate": 0.9047619047619048,
            },
            {
                "char": "Y",
                "frequency": 2,
                "cumsum": 40,
                "cumsum_rate": 0.9523809523809523,
            },
            {"char": "Z", "frequency": 2, "cumsum": 42, "cumsum_rate": 1.0},
        ]

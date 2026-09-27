from data_refiner.core import recorder
from data_refiner.ops.builtin.explode import Explode


class TestExplodeBuiltinOp:
    def test_array_explode(self, spark):
        test_data = [(1, "s", [1, 2, 3])]
        df = spark.createDataFrame(test_data, ["id", "text", "array"])
        recorder.record("test.df", df)
        mapper = Explode(
            input_df="test.df",
            output_df="test.df_explode",
            field="array",
            exploded_cols=["exploded_value"],
            show=True,
            drop=["array"],
        )
        df_processed = mapper.process(spark=spark)
        assert [row.asDict() for row in df_processed.collect()] == [
            {"id": 1, "text": "s", "exploded_value": 1},
            {"id": 1, "text": "s", "exploded_value": 2},
            {"id": 1, "text": "s", "exploded_value": 3},
        ]

    def test_map_explode(self, spark):
        test_data = [(1, "s", {"a": 1, "b": 2, "c": 3})]
        df = spark.createDataFrame(test_data, ["id", "text", "map"])
        recorder.record("test.df", df)
        mapper = Explode(
            input_df="test.df",
            output_df="test.df_explode",
            field="map",
            exploded_cols=["k", "v"],
            show=True,
            drop=["map"],
        )
        df_processed = mapper.process()
        assert [row.asDict() for row in df_processed.collect()] == [
            {"id": 1, "text": "s", "k": "a", "v": 1},
            {"id": 1, "text": "s", "k": "b", "v": 2},
            {"id": 1, "text": "s", "k": "c", "v": 3},
        ]

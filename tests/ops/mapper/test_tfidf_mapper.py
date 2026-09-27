import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.tfidf_mapper import TfIdfMapper


@pytest.mark.usefixtures("spark")
class TestTfIdfMapper:
    def test_tfidf_calculation(self, spark):
        test_data = [
            (1, ["how", "are", "you"]),
            (2, ["are", "you", "ok", "ok"]),
            (3, ["hello", "thank", "you"]),
            (4, None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "tokens"])
        recorder.record("test.df", df)

        mapper = TfIdfMapper(
            input_df="test.df",
            output_df="test.df_tfidf",
            field="tokens",
            output_field="tfidf",
            show=True,
        )
        df = mapper.process(spark=spark)
        df_list = [x.asDict() for x in df.collect()]
        for item in df_list:
            item["tfidf"] = [list(x) for x in item["tfidf"]]
        assert df_list == [
            {
                "__id__": 1,
                "tokens": ["how", "are", "you"],
                "tfidf": [
                    ["how", 1, 1, 1.6931],
                    ["are", 1, 2, 1.2877],
                    ["you", 1, 3, 1.0],
                ],
            },
            {
                "__id__": 2,
                "tokens": ["are", "you", "ok", "ok"],
                "tfidf": [
                    ["ok", 2, 1, 3.3863],
                    ["are", 1, 2, 1.2877],
                    ["you", 1, 3, 1.0],
                ],
            },
            {
                "__id__": 3,
                "tokens": ["hello", "thank", "you"],
                "tfidf": [
                    ["hello", 1, 1, 1.6931],
                    ["thank", 1, 1, 1.6931],
                    ["you", 1, 3, 1.0],
                ],
            },
            {
                "__id__": 4,
                "tokens": None,
                "tfidf": [],
            },
        ]

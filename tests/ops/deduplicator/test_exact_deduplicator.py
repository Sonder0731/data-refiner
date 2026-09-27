import pytest

from data_refiner.core import recorder
from data_refiner.ops.deduplicator.exact_deduplicator import ExactDeduplicator


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        ("hello world!", 0.1),
        ("hello world!", 0.2),
        ("hello world!1", 0.1),
        ("hello world!2", 0.1),
    ]
    df = spark.createDataFrame(test_data, ["content", "score"])
    df.cache()
    recorder.record("input_df", df)
    return recorder


# @pytest.mark.usefixtures("spark", "records")
class TestExactDeduplicator:
    def test_mode_dedup_with_normal_comparison_function(self, records):
        mapper = ExactDeduplicator(
            input_df="input_df",
            output_df="exact_deduplicator.df",
            field="content",
            comparison_function="""
            if r1["score"] >= r2["score"]:
                return r1
            else:
                return r2
            """,
            show=True,
        )
        df = mapper.process()
        rdd = df.rdd.collect()
        check_data = [row["score"] for row in rdd if row["content"] == "hello world!"]
        assert len(set([row["gid"] for row in rdd])) == 3
        assert len(check_data) == 1
        assert check_data[0] == 0.2

    def test_mode_dup_with_normal_comparison_function(self, records):
        mapper = ExactDeduplicator(
            input_df="input_df",
            output_df="exact_deduplicator.df",
            field="content",
            mode="dup",
            comparison_function="""
            if r1["score"] >= r2["score"]:
                return r1
            else:
                return r2
            """,
            show=True,
        )
        df = mapper.process()
        rdd = df.rdd.collect()
        check_data = [row["score"] for row in rdd if row["content"] == "hello world!"]
        assert len(set([row["gid"] for row in rdd])) == 1
        assert len(check_data) == 1
        assert check_data[0] == 0.1

    def test_mode_dedup_with_lambda_comparison_function(self, records):
        mapper = ExactDeduplicator(
            input_df="input_df",
            output_df="exact_deduplicator.df",
            field="content",
            comparison_function="""return r1 if r1["score"] >= r2["score"] else r2""",
            show=True,
        )
        df = mapper.process()
        rdd = df.rdd.collect()
        assert len(set([row["gid"] for row in rdd])) == 3
        assert len([row["content"] for row in rdd if row["content"] == "hello world!"]) == 1

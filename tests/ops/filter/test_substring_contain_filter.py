import pytest
from data_refiner.core import recorder
from data_refiner.core.registry import registry
from data_refiner.ops.filter.substring_contain_filter import SubstringContainFilter


@pytest.fixture(autouse=True)
def _register_operator():
    registry.register("substring_contain_filter", SubstringContainFilter)


def test_substring_contain_filter_basic(spark):
    data = [
        ("1", "apple cake"),
        ("2", "pear tart"),
        ("3", "pineapple chunk"),
        ("4", None),
    ]
    df = spark.createDataFrame(data, ["id", "food"])
    recorder.record("input_rec", df)

    op = SubstringContainFilter(
        input_df="input_rec", output_df="output_rec", field="food", substring="pine", show=True
    )

    op.process(spark=spark)
    result_df = recorder.load("output_rec")

    rows = [(r["id"], r["food"]) for r in result_df.collect()]
    expected = [("3", "pineapple chunk")]
    assert sorted(rows) == sorted(expected)

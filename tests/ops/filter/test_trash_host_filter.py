import pytest
from pathlib import Path

from data_refiner.core import recorder
from data_refiner.core.dependency import FilterLevel
from data_refiner.ops.filter.trash_host_filter import TrashHostFilter
from data_refiner.utils.tools import get_env_var
from tests.tools import assert_same_by_dict

@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        ("gtradersoft.com",),
        ("bbc.com",),
        ("aiwesnww112331mxx.com",),
        (None,),
    ]
    df = spark.createDataFrame(test_data, ["url"]).repartition(1)
    df.cache()
    recorder.record("url_df", df)
    return recorder


class TestTrashHostFilter:
    def test_filter_with_tag(self, spark, records):
        filter = TrashHostFilter(
            input_df="url_df",
            output_df="trash_host.df_tag",
            field="url",
            mode=FilterLevel.TAG,
            tag_field="__stay__",
            trash_host_paths=[
                "data-refiner-runtime-resources/data/FadeMindhosts.txt",
                "data-refiner-runtime-resources/data/KADhosts.txt",
            ],
            show=True,
            cache="disk",
        )
        df = filter.process(spark=spark)
        assert [line["__stay__"] for line in df.rdd.collect()] == [False, True,True, True]

    def test_filter_with_filter(self, spark, records):
        filter = TrashHostFilter(
            input_df="url_df",
            output_df="trash_host.df_tag",
            field="url",
            # mode=FilterLevel.TAG,
            tag_field="__stay__",
            trash_host_paths=[
                "data-refiner-runtime-resources/data/FadeMindhosts.txt",
                "data-refiner-runtime-resources/data/KADhosts.txt",
            ],
            show=True,
        )
        df = filter.process(spark=spark)
        assert [line["url"] for line in df.rdd.collect()] == ["bbc.com","aiwesnww112331mxx.com", None]

    def test_mode_with_tag_and_filter(self, spark, records):
        filter = TrashHostFilter(
            input_df="url_df",
            output_df="trash_host.df_tag",
            field="url",
            mode=FilterLevel.TAG_AND_FILTER,
            tag_field="__stay__",
            trash_host_paths=[
                "data-refiner-runtime-resources/data/FadeMindhosts.txt",
                "data-refiner-runtime-resources/data/KADhosts.txt",
            ],
            show=True,
        )
        df = filter.process(spark=spark)
        assert [line["url"] for line in df.rdd.collect()] == ["bbc.com","aiwesnww112331mxx.com", None]
        assert [line["__stay__"] for line in df.rdd.collect()] == [True,True, True]

    def test_additional_trash_hosts(self, spark, records):
        trash_host_path = Path(get_env_var("ADDITIONAL_TRASH_HOSTS"))
        spark.sparkContext.addFile(str(trash_host_path))
        filter = TrashHostFilter(
            input_df="url_df",
            output_df="trash_host.df_tag",
            field="url",
            mode=FilterLevel.TAG_AND_FILTER,
            bloom_error_rate=1e-6,
            tag_field="__stay__",
            trash_host_paths=[
                "data-refiner-runtime-resources/data/FadeMindhosts.txt",
                "data-refiner-runtime-resources/data/KADhosts.txt",
                trash_host_path.name,
            ],
            show=True,
        )
        df = filter.process(spark=spark)
        assert [line["url"] for line in df.rdd.collect()] == ["bbc.com", None]

    def test_only_user_trash_hosts(self, spark, records):
        trash_host_path = Path(get_env_var("ADDITIONAL_TRASH_HOSTS"))
        spark.sparkContext.addFile(str(trash_host_path))
        filter = TrashHostFilter(
            input_df="url_df",
            output_df="trash_host.df_tag",
            field="url",
            mode=FilterLevel.TAG_AND_FILTER,
            bloom_error_rate=1e-6,
            tag_field="__stay__",
            trash_host_paths=[
                trash_host_path.name,
            ],
            show=True,
        )
        df = filter.process(spark=spark)
        assert [line["url"] for line in df.rdd.collect()] == ["gtradersoft.com","bbc.com", None]


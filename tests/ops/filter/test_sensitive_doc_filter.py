import pytest
from pathlib import Path

from data_refiner.core import recorder
from data_refiner.core.dependency import FilterLevel
from data_refiner.ops.filter.sensitive_doc_filter import SensitiveDocFilter
from data_refiner.utils.tools import get_env_var


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        (
            1,
            "少看成人电影，警惕裸聊陷阱",
        ),
        (
            2,
            "少看成人电影",
        ),
        (
            3,
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ),
        (
            4,
            None,
        ),
    ]
    df = spark.createDataFrame(test_data, ["id", "content"]).repartition(1)
    df.cache()
    recorder.record("shared_df", df)
    return recorder


class TestSensitiveDocFilter:
    def test_default_resource_path_is_cluster_relative(self):
        filter = SensitiveDocFilter(
            input_df="shared_df",
            output_df="sensitive_doc_filter.df",
            field="content",
        )

        assert filter.sensitive_keyword_paths == [
            Path("data-refiner-runtime-resources/data/SensitiveLexicon.json")
        ]

    def test_filter_with_tag(self, spark, records):
        filter = SensitiveDocFilter(
            input_df="shared_df",
            output_df="sensitive_doc_filter.df_tag",
            field="content",
            mode=FilterLevel.TAG,
            tag_field="__stay__",
            keyword_limits=2,
            targeted_keywords_field="targeted_keywords",
            show=True,
        )
        df = filter.process(spark=spark)
        ids = [(line["id"], line["__stay__"]) for line in df.rdd.collect()]
        assert ids == [(1, False), (2, True), (3, True), (4, True)]

    def test_filter_with_filter(self, spark, records):
        filter = SensitiveDocFilter(
            input_df="shared_df",
            output_df="sensitive_doc_filter.df_tag",
            field="content",
            # mode=FilterLevel.TAG,
            keyword_limits=2,
            targeted_keywords_field="targeted_keywords",
            show=True,
        )
        df = filter.process(spark=spark)
        assert [line["id"] for line in df.rdd.collect()] == [2, 3, 4]

    def test_mode_with_tag_and_filter(self, spark, records):
        filter = SensitiveDocFilter(
            input_df="shared_df",
            output_df="sensitive_doc_filter.df_tag",
            field="content",
            mode=FilterLevel.TAG_AND_FILTER,
            tag_field="__stay__",
            keyword_limits=2,
            targeted_keywords_field="targeted_keywords",
            show=True,
        )
        df = filter.process(spark=spark)
        assert [line["id"] for line in df.rdd.collect()] == [2, 3, 4]
        assert [line["__stay__"] for line in df.rdd.collect()] == [True, True, True]

    def test_additional_files(self,spark, records):
        additional_keywords_path = Path(get_env_var("ADDITIONAL_SENSITIVE_KEYWORDS"))
        spark.sparkContext.addFile(str(additional_keywords_path))
        filter = SensitiveDocFilter(
            input_df="shared_df",
            output_df="sensitive_doc_filter.df_tag",
            sensitive_keyword_paths=["data-refiner-runtime-resources/data/SensitiveLexicon.json",additional_keywords_path.name],
            field="content",
            mode=FilterLevel.TAG_AND_FILTER,
            tag_field="__stay__",
            keyword_limits=0,
            targeted_keywords_field="targeted_keywords",
            show={"truncate":False},
        )
        df = filter.process(spark=spark)
        assert [line["id"] for line in df.rdd.collect()] == [4]

    def test_only_user_files(self,spark, records):
        additional_keywords_path = Path(get_env_var("ADDITIONAL_SENSITIVE_KEYWORDS"))
        spark.sparkContext.addFile(str(additional_keywords_path))
        filter = SensitiveDocFilter(
            input_df="shared_df",
            output_df="sensitive_doc_filter.df_tag",
            sensitive_keyword_paths=[additional_keywords_path.name],
            field="content",
            mode=FilterLevel.TAG_AND_FILTER,
            tag_field="__stay__",
            keyword_limits=0,
            targeted_keywords_field="targeted_keywords",
            show={"truncate":False},
        )
        df = filter.process(spark=spark)
        assert [line["id"] for line in df.rdd.collect()] == [1,2,4]

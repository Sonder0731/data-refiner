from data_refiner.core import recorder
from data_refiner.ops.mapper.url_normalization_mapper import UrlNormalizationMapper
from tests.tools import assert_same_by_dict


class TestUrlNormalizationMapper:
    def test_normalize_scheme_host_port(self, spark):
        df = spark.createDataFrame(
            [(1, "HTTP://Example.COM:80/path?b=2&a=1#frag"), (2, None)], ["__id__", "url"]
        )
        recorder.record("test.url_df", df)
        mapper = UrlNormalizationMapper(
            input_df="test.url_df",
            output_df="test.url_out",
            field="url",
            output_field="normalized_url",
            show=True,
            drop=["url"],
        )
        result = mapper.process(spark=spark)
        assert_same_by_dict(
            result,
            [
                {"__id__": 1, "normalized_url": "http://example.com/path?a=1&b=2"},
                {"__id__": 2, "normalized_url": None},
            ],
        )

    def test_normalize_https_default_port(self, spark):
        df = spark.createDataFrame([(1, "https://Example.COM:443/page")], ["__id__", "url"])
        recorder.record("test.url_df2", df)
        mapper = UrlNormalizationMapper(
            input_df="test.url_df2",
            output_df="test.url_out2",
            field="url",
            output_field="normalized_url",
            show=True,
            drop=["url"],
        )
        result = mapper.process(spark=spark)
        assert_same_by_dict(result, [{"__id__": 1, "normalized_url": "https://example.com/page"}])

    def test_sort_query_params(self, spark):
        df = spark.createDataFrame([(1, "http://example.com/search?z=3&a=1")], ["__id__", "url"])
        recorder.record("test.url_df3", df)
        mapper = UrlNormalizationMapper(
            input_df="test.url_df3",
            output_df="test.url_out3",
            field="url",
            output_field="normalized_url",
            show=True,
            drop=["url"],
        )
        result = mapper.process(spark=spark)
        assert_same_by_dict(
            result, [{"__id__": 1, "normalized_url": "http://example.com/search?a=1&z=3"}]
        )

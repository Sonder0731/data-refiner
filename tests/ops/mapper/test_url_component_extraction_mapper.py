import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.url_component_extraction_mapper import UrlComponentExtractionMapper
from tests.tools import assert_same_by_dict


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        (
            1,
            "https://aistudio.google.com",
        ),
        (
            2,
            "https://user:pass@www.example.com:8080/path/to/resource?name=Alice&age=30#section-1",
        ),
        (3, None),
    ]
    df = spark.createDataFrame(test_data, ["id", "url"]).repartition(1)
    df.cache()
    recorder.record("url_df", df)
    return recorder


@pytest.mark.usefixtures("spark", "records")
class TestUrlComponentExtractionMapper:
    def test_extract(self):
        mapper = UrlComponentExtractionMapper(
            input_df="url_df",
            output_df="url_components.df",
            field="url",
            required_components=["scheme", "path", "port", "query"],
            show=True,
            count=True,
            cache="disk",
            drop=["url"],
        )
        df = mapper.process()
        assert_same_by_dict(
            df,
            [
                {"id": 1, "scheme": "https", "path": "", "port": "443", "query": ""},
                {
                    "id": 2,
                    "scheme": "https",
                    "path": "/path/to/resource",
                    "port": "8080",
                    "query": "name=Alice&age=30",
                },
                {"id": 3, "scheme": None, "path": None, "port": None, "query": None},
            ],
            id_column="id",
        )

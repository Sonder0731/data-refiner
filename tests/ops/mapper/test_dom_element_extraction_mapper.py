import select

from data_refiner.core import recorder
from data_refiner.ops.mapper.dom_element_extraction_mapper import DomElementExtractionMapper
from pyspark.sql import SparkSession
from tests.tools import assert_same_by_dict


class TestDomElementExtractionMapper:
    def test_css_selector_extraction(self, spark: SparkSession):
        test_data = [
            (1, "<html><body><div class='content'>Hello <span>world</span></div></body></html>"),
            (
                2,
                "<html><body><p>Ignore this</p><div class='content'>Hi <em>there</em></div></body></html>",
            ),
            (3, "<html><body><div>No match here</div></body></html>"),
            (4, None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "html_content"])
        recorder.record("test.df", df)
        mapper = DomElementExtractionMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="html_content",
            output_field="extracted_html",
            css_selector=".content",
            show=True,
            select=["__id__", "extracted_html"],
        )
        df_processed = mapper.process(spark=spark)
        expected = [
            {"__id__": 1, "extracted_html": '<div class="content">Hello <span>world</span></div>'},
            {"__id__": 2, "extracted_html": '<div class="content">Hi <em>there</em></div>'},
            {"__id__": 3, "extracted_html": ''},
            {"__id__": 4, "extracted_html": None},
        ]
        assert_same_by_dict(df_processed, expected)

    def test_xpath_selector_extraction(self, spark: SparkSession):
        test_data = [
            (1, "<html><body><ul><li>Item 1</li><li class='target'>Item 2</li></ul></body></html>"),
            (2, "<html><body><div><p class='target'>Paragraph</p></div></body></html>"),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "html_content"])
        recorder.record("test.df", df)
        mapper = DomElementExtractionMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="html_content",
            output_field="extracted_html",
            xpath_selector="//li[@class='target'] | //p[@class='target']",
            show=True,
            select=["__id__", "extracted_html"],
        )
        df_processed = mapper.process(spark=spark)
        expected = [
            {"__id__": 1, "extracted_html": '<li class="target">Item 2</li>'},
            {"__id__": 2, "extracted_html": '<p class="target">Paragraph</p>'},
        ]
        assert_same_by_dict(df_processed, expected)

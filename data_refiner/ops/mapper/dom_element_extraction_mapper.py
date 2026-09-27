from pydantic import BaseModel, Field
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params, check_column_schema
from parsel import Selector


@processing_operator
class DomElementExtractionMapper(SimpleMapper, OperatorConstraint):
    """
    Extracts specified DOM elements and their subtrees from HTML content using CSS selectors or XPath, outputting the serialized HTML string of the matched elements. 使用 CSS 选择器或 XPath 从 HTML 内容中提取指定的 DOM 元素及其子树，并输出匹配元素的序列化 HTML 字符串
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `css_selector` -> CSS Extraction Rules CSS 提取规则: When this parameter is provided, the partition operator evaluates this specific CSS path against the source HTML column specified by `field`. 当提供该参数时，分区算子会针对由 `field` 指定的源 HTML 列评估该特定的 CSS 路径。
* `xpath_selector` -> XPath Extraction Rules XPath 提取规则: When this parameter is provided, the partition operator evaluates this specific XPath expression against the source HTML column specified by `field`. 当提供该参数时，分区算子会针对由 `field` 指定的源 HTML 列评估该特定的 XPath 表达式。
* `field` -> Input Column 输入列: This parameter specifies the target column containing the raw HTML strings from which DOM elements will be extracted. 该参数指定包含原始 HTML 字符串的目标列，将从该列中提取 DOM 元素。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the serialized HTML string of the matched DOM elements will be stored. 该参数定义了存储匹配到的 DOM 元素序列化 HTML 字符串的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to execute the `extract_elements` method across partitions via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在分区之间执行 `extract_elements` 方法。
* Within the partition function, conditional logic switches between `selector.css()` and `selector.xpath()` based on the defined parameters, and a new key-value pair represented by `output_field` is dynamically added to each row dictionary. 在分区函数内部，条件逻辑根据定义的参数在 `selector.css()` 和 `selector.xpath()` 之间切换，并向每个行字典中动态添加了一个由 `output_field` 表示的新键值对。
* The transformed RDD is re-converted back into a DataFrame structural format using the `toDF()` method, which maps the updated dictionary schemas onto a new DataFrame representation. 转换后的 RDD 通过 `toDF()` 方法重新转换为 DataFrame 结构格式，该方法将更新后的字典 Schema 映射到新的 DataFrame 表示中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。"""

    class DomElementExtractionMapperParams(BaseModel):
        css_selector: str = Field(default="", description="CSS selector to extract DOM elements.")
        xpath_selector: str = Field(
            default="", description="XPath selector to extract DOM elements."
        )

    config = DomElementExtractionMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(
            self.config,
            kwargs,
        )
        self.css_selector = params.css_selector
        self.xpath_selector = params.xpath_selector

        if self.css_selector and self.xpath_selector:
            raise ValueError("Only one of css_selector or xpath_selector should be provided.")
        if not self.css_selector and not self.xpath_selector:
            raise ValueError("Either css_selector or xpath_selector must be provided.")

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())

        css_selector = self.css_selector
        xpath_selector = self.xpath_selector

        def extract_elements(partition):
            for row in partition:
                row_dict = row.asDict()
                html_content = row_dict.get(self.field, "")

                if html_content is None:
                    extracted_html = None
                else:
                    selector = Selector(text=html_content)

                    if css_selector:
                        elements = selector.css(css_selector)
                    else:
                        elements = selector.xpath(xpath_selector)

                    # Serialize all matched elements to HTML strings
                    extracted_fragments = [elem.get() for elem in elements]
                    extracted_html = "".join(extracted_fragments)

                row_dict[self.output_field] = extracted_html
                yield Row(**row_dict)

        result_rdd = df.rdd.mapPartitions(extract_elements)
        result_df = result_rdd.toDF()

        return result_df

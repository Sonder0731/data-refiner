import trafilatura
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pydantic import BaseModel

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


def _extract(html: str) -> str:
    if html is None:
        return None
    return trafilatura.extract(html)


@processing_operator
class HtmlContentExtractMapper(SimpleMapper, OperatorConstraint):
    """
    A common html content extraction ops by trafilatura library. 使用 trafilatura 库的提取 HTML 内容
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the source column containing raw HTML strings from which text content will be extracted. 该参数指定包含原始 HTML 字符串的源列，将从该列中提取文本内容。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the extracted clean text string will be stored. 该参数定义了存储提取出的干净文本字符串的新列的列名。

### Schema Transformation Process Schema 转换过程
* A User Defined Function (UDF) named `extract_udf` is registered, which wraps the core `_extract` logic using the `trafilatura` library and explicitly sets its return type to `StringType()`. 注册了一个名为 `extract_udf` 的用户自定义函数（UDF），该函数使用 `trafilatura` 库封装了核心的 `_extract` 逻辑，并将其返回类型明确设置为 `StringType()`。
* The `withColumn` operator is executed on the input DataFrame to append a new column defined by `output_field`, while preserving all existing columns in their original sequence. 在输入 DataFrame 上执行 `withColumn` 算子以追加由 `output_field` 定义的新列，同时按原始顺序保留所有现有列。
* No intermediate columns are deleted, and no data types of the existing schema attributes are modified during this lifecycle step. 在此生命周期步骤中，没有删除任何中间列，也没有修改任何现有 Schema 属性的数据类型。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are retained with their initial data types and schema structures. 输入 DataFrame 中的所有原始列均被保留，并保持其初始数据类型和 Schema 结构。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly structured as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确构建为 `StringType`。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        extract_udf = F.udf(_extract, StringType())
        res_df = df.withColumn(self.output_field, extract_udf(F.col(self.field)))
        return res_df

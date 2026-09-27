import re

from pydantic import BaseModel, Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params, check_column_schema


@processing_operator
class FilterLineByRegexMapper(SimpleMapper, OperatorConstraint):
    """
    Filter the lines(split by newline) in a text based on a regular expression. if the line matched by the regular expression, the line will be removed. 根据正则表达式过滤文本中按换行符分割的行。如果某一行匹配该正则表达式，则移除该行
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `regex` -> Matching Pattern 匹配模式: This parameter provides the regular expression string used within the execution function to evaluate and filter individual lines of text. 该参数提供了在执行函数中使用的正则表达式字符串，用于评估和过滤文本的每一行。
* `field` -> Input Column 输入列: This parameter specifies the source text column whose multiline string content will be split and filtered line by line. 该参数指定源文本列，其多行字符串内容将按行进行拆分和过滤。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the reconstructed text string (excluding matched lines) will be stored. 该参数定义了存储重构后的文本字符串（不包含匹配行）的新列的列名。

### Schema Transformation Process Schema 转换过程
* A User Defined Function (UDF) named `line_regex_filter_udf` is registered, wrapping the core processing logic of `_line_regex_filter` with an explicit return type of `StringType()`. 注册了一个名为 `line_regex_filter_udf` 的用户自定义函数（UDF），封装了 `_line_regex_filter` 的核心处理逻辑，并具有明确的返回类型 `StringType()`。
* The `withColumn` operator is executed on the input DataFrame to append a new column defined by `output_field`, which populates the filtered text results while maintaining all existing columns in their original positions. 在输入 DataFrame 上执行 `withColumn` 算子以逃加由 `output_field` 定义的新列，该列填充了过滤后的文本结果，同时保持所有现有列在原始位置不变。
* No intermediate columns are dropped, and no data types of the existing schema attributes are modified during this lifecycle step. 在此生命周期步骤中，没有删去任何中间列，也没有修改现有 Schema 属性的数据类型。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved with their initial data types and column index structures. 输入 DataFrame 中的所有原始列均被保留，并保持其初始数据类型和列索引结构。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly structured as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确构建为 `StringType`。"""

    class FilterLineByRegexMapperParams(BaseModel):
        regex: str = Field(..., description="Regular expression to match the lines.")

    config = FilterLineByRegexMapperParams
    __slots__ = ["regex"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.regex = params.regex

    def _line_regex_filter(self, text: str) -> str:
        if text is None:
            return None
        lines = text.split("\n")
        return "\n".join([line for line in lines if not re.search(self.regex, line)])

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        input_df: DataFrame = recorder.load(self.input_df)
        check_column_schema(input_df, self.field, StringType())
        line_regex_filter_udf = F.udf(self._line_regex_filter, StringType())
        res_df = input_df.withColumn(self.output_field, line_regex_filter_udf(F.col(self.field)))
        return res_df

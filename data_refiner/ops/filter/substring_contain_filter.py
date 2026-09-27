from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType, StringType

from data_refiner.core import recorder
from data_refiner.core.dependency import PERSIST_LEVEL
from data_refiner.core.meta_operator import Filter, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import (
    check_params,
    check_column_schema,
    return_df_by_filter_level,
)
from loguru import logger


@processing_operator
class SubstringContainFilter(Filter, OperatorConstraint):
    """
    Filter the dataframe by checking if a text field contains a specific substring. 通过检查文本字段是否包含特定子字符串来过滤 DataFrame
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly relies on the specific source string column specified by `field`. 该算子直接依赖于由 `field` 指定的特定源字符串列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is verified by the `check_column_schema` method before any processing. 由 `field` 指定的列必须为 `StringType()`，在进行任何处理前由 `check_column_schema` 方法进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Inspected Text Column 被检查文本列: This parameter designates the target column evaluated by the Spark column string verification expression. 该参数指定由 Spark 列字符串验证表达式评估的目标列。
* `substring` -> Matching Pattern 匹配模式: This parameter supplies the literal string value utilized by the `.contains()` operator to test the designated `field`. 该参数提供了由 `.contains()` 算子使用的字面量字符串值，用于测试指定的 `field`。
* `tag_field` -> Conditional Evaluation Column 条件评估列: This parameter names the appended intermediate boolean column that holds the outcome of the substring match. 该参数命名了追加的中间布尔列，用于保存子字符串匹配的结果。
  * When `field` contains `substring` 当 `field` 包含 `substring` 时: The entry value in `tag_field` evaluates to `True`. `tag_field` 中的条目值解析为 `True`。
  * When `field` does not contain `substring` 当 `field` 不包含 `substring` 时: The entry value in `tag_field` evaluates to `False`. `tag_field` 中的条目值解析为 `False`。
* `mode` -> Row Retention Filter 过滤行保留模式: This parameter dictates the row-slicing actions applied within `return_df_by_filter_level` based on the values in `tag_field`. 该参数决定了在 `return_df_by_filter_level` 中基于 `tag_field` 中的值所执行的行切片操作。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A new boolean indicator column defined by `tag_field` is appended to the schema using the `withColumn` operator, containing the evaluated states of the `.contains()` expression. 一个由 `tag_field` 定义的新布尔指示列通过 `withColumn` 算子被追加到 Schema 中，包含 `.contains()` 表达式的评估状态。
* Type Modifications 类型改变: The preexisting operational columns maintain their initial data types, and no inplace type transformations occur. 原有业务列保持其初始数据类型，未发生任何就地类型转换。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is sent to `return_df_by_filter_level`, where it might be structurally stripped out of the final DataFrame depending on the filtering level routine. 中间 `tag_field` 列被发送至 `return_df_by_filter_level`，在那里它可能会根据过滤级别例程从最终 DataFrame 中被结构化地剥离。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame returns the baseline structural columns identical to the input schema, whereas the temporary indicator column `tag_field` is decoupled or omitted from the final presentation. 最终的 DataFrame 返回与输入 Schema 相同的基线结构列，而临时指示列 `tag_field` 则从最终呈现中被解耦或省略。
* Final Column Data Types 最终列数据类型: Every baseline column preserves its incoming operational data type (e.g., `field` strictly remains `StringType()`) without structural modifications. 每一个基线列都保留其输入的业务数据类型（例如，`field` 严格保持为 `StringType()`），未发生结构性修改。"""

    class SubstringContainFilterParams(BaseModel):
        substring: str = Field(
            default=None,
            description="substring (str): The substring to search for within the text field.",
        )

    config = SubstringContainFilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.substring = params.substring

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        logger.info(
            f"Starting substring_contain_filter operation on field {self.field} with substring '{self.substring}'"
        )
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())

        condition_col = F.col(self.field).contains(self.substring)
        df = df.withColumn(self.tag_field, condition_col)
        self.persist_tmps(df, "disk")
        res_df = return_df_by_filter_level(
            df,
            self.tag_field,
            self.mode,
        )

        # Record the output dataframe
        if res_df is not None:
            recorder.record(self.output_df, res_df)

        logger.info(f"Finished substring_contain_filter operation")
        return res_df

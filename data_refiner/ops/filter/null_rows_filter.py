from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, functions as F

from data_refiner.core import recorder
from data_refiner.core.dependency import PERSIST_LEVEL
from data_refiner.core.meta_operator import Filter, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import (
    check_params,
    return_df_by_filter_level,
)


@processing_operator
class NullRowsFilter(Filter, OperatorConstraint):
    """
    Remove rows that contain any null value across specified or all columns. 删除指定列或所有列中包含空值的行
    """

    CONSTRAINT = """### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operation depends on the columns specified in `columns_to_check`. If `columns_to_check` is `None`, it depends on all columns present in the input DataFrame. 该操作依赖于 `columns_to_check` 中指定的列。如果 `columns_to_check` 为 `None`，则依赖于输入 DataFrame 中存在的所有列。
* Required Data Types 要求的数据类型: All target columns to be checked support any valid Spark DataType, as the `isNotNull()` operator is universally applicable. 所有被检查的目标列支持任何有效的 Spark 数据类型，因为 `isNotNull()` 算子是普遍适用的。

### Argument and Column Mapping 参数与列的映射
* `columns_to_check` -> Checked Columns 被检查列: This parameter explicitly defines the subset of columns to be evaluated for null values. 该参数显式定义了需要评估空值的列子集。
  * When `columns_to_check` is provided 当提供 `columns_to_check` 时: The operator filters rows based only on the specified list of columns. 算子仅基于指定的列列表过滤行。
  * When `columns_to_check` is `None` 当 `columns_to_check` 为 `None` 时: The operator dynamically binds to all columns in the input DataFrame schema. 算子动态绑定到输入 DataFrame Schema 中的所有列。
* `tag_field` -> Intermediate Tag Column 中间标记列: This parameter specifies the name of the intermediate boolean column used to store the filter condition evaluation result. 该参数指定用于存储过滤条件评估结果的中间布尔列的名称。
* `mode` -> Filtering Strategy 行过滤策略: This parameter determines how rows are retained or discarded based on the boolean value in `tag_field`. 该参数决定如何基于 `tag_field` 中的布尔值保留或丢弃行。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A temporary boolean column named after `tag_field` is appended to the DataFrame via the `withColumn` operator, holding the result of the combined non-null conditions. 一个以 `tag_field` 命名的临时布尔列通过 `withColumn` 算子被追加到 DataFrame 中，保存组合非空条件的结果。
* Elimination of Intermediate Columns 中间列的消除: During the execution of `return_df_by_filter_level`, the intermediate `tag_field` column may be dropped or handled depending on the internal logic of the filtering level helper, returning the final cleansed dataset. 在 `return_df_by_filter_level` 的执行过程中，中间的 `tag_field` 列可能会根据过滤级别辅助函数的内部逻辑被删除或处理，从而返回最终清洗后的数据集。
* Type Modifications 类型改变: No existing columns undergo data type modifications during the lifecycle of this operator. 在该算子的生命周期内，没有任何现有列会经历数据类型改变。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame contains the same set of business schema columns as the input DataFrame, while the temporary `tag_field` column is processed and excluded from the definitive output schema. 最终的 DataFrame 包含与输入 DataFrame 相同的业务 Schema 列集合，而临时 `tag_field` 列已被处理并排除在最终输出 Schema 之外。
* Final Column Data Types 最终列数据类型: All retained columns strictly preserve their original input data types without any modification. 所有保留的列严格保持其原始的输入数据类型，未发生任何改变。"""

    class NullRowsFilterParams(BaseModel):
        fields: list[str] = Field(
            default=None,
            description="List of columns to check for null values. If None, checks all columns.",
        )

    config = NullRowsFilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        del self.field
        params = check_params(self.config, kwargs)
        self.fields = params.fields

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)

        # Determine which columns to check for nulls
        columns_to_check = self.fields if self.fields is not None else df.columns

        # Create a condition that checks if ALL specified columns are NOT null
        # In Spark, ISNULL returns True for null values, so we negate it
        non_null_condition = F.lit(True)
        for col_name in columns_to_check:
            non_null_condition = non_null_condition & F.col(col_name).isNotNull()

        # Apply the condition to create the tag field
        df = df.withColumn(self.tag_field, non_null_condition)

        self.persist_tmps(df, "disk")

        # Return dataframe based on the filter mode
        res_df = return_df_by_filter_level(
            df,
            self.tag_field,
            self.mode,
        )
        return res_df

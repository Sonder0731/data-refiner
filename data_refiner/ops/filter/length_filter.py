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


@processing_operator
class LengthFilter(Filter, OperatorConstraint):
    """
    Filter the text by its length. 按文本长度筛选文本
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly depends on the target text column specified by `field`. 该算子直接依赖于由 `field` 指定的目标文本列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is strictly validated by the `check_column_schema` method before execution. 由 `field` 指定的列必须为 `StringType()`，在执行前通过 `check_column_schema` 方法进行严格验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Inspected Text Column 被检查文本列: This parameter maps to the specific column whose text character length is calculated and validated against the length bounds. 该参数映射到特定的列，其文本字符长度将被计算并根据长度边界进行验证。
* `min_length` & `max_length` -> Range Constraints 范围约束: These parameters define the conditional logic thresholds applied to the string length evaluation of the mapped `field`. 这些参数定义了应用于被映射 `field` 的字符串长度评估的条件逻辑阈值。
* `tag_field` -> Result Boolean Column 结果布尔列: This parameter defines the name of the new column created to store the boolean evaluation result of the UDF filter. 该参数定义了新创建的列名，用于存储 UDF 过滤器布尔评估的结果。
  * When text length satisfies `min_length <= length <= max_length` 当文本长度满足 `min_length <= length <= max_length` 时: The row value in `tag_field` resolves to `True`. `tag_field` 中的行值解析为 `True`。
  * When text length violates the bounds 当文本长度违反边界时: The row value in `tag_field` resolves to `False`. `tag_field` 中的行值解析为 `False`。
* `mode` -> Filtering Action Mode 过滤行为模式: This parameter governs how rows are pruned or retained based on the boolean status inside `tag_field` via the `return_df_by_filter_level` routine. 该参数控制如何通过 `return_df_by_filter_level` 例程基于 `tag_field` 内的布尔状态对行进行剪枝或保留。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A user-defined function `length_filter_udf` mapping to `BooleanType()` is executed via `withColumn`, appending a new intermediate boolean column named after `tag_field` to the current DataFrame. 一个映射到 `BooleanType()` 的用户自定义函数 `length_filter_udf` 通过 `withColumn` 被执行，向当前 DataFrame 追加一个以 `tag_field` 命名的全新中间布尔列。
* Type Modifications 类型改变: The schema of the existing columns remains unmodified, while the schema structural width is expanded by exactly one `BooleanType` column. 现有列的 Schema 保持未修改状态，而 Schema 的结构宽度恰好扩展了一个 `BooleanType` 列。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is passed into `return_df_by_filter_level`, which may drop this temporary indicator column prior to delivering the final structural output. 中间 `tag_field` 列被传入 `return_df_by_filter_level` 中，该函数可能会在交付最终结构输出之前删除此临时指示列。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame maintains the identical schema structure as the input dataset, while the generated `tag_field` is stripped or retained depending on the downstream configuration of the filter level utility. 最终的 DataFrame 保持与输入数据集完全相同的 Schema 结构，而生成的 `tag_field` 则取决于过滤级别工具的下游配置而被剥离或保留。
* Final Column Data Types 最终列数据类型: Every column present in the final output retains its initial data type (e.g., `field` strictly remains `StringType()`) without any type distortion. 最终输出中存在的每一列都保留其初始数据类型（例如，`field` 严格保持为 `StringType()`），没有任何类型畸变。"""

    class LengthFilterParams(BaseModel):
        min_length: int = Field(
            default=None, description="min_length (int): Minimum length of the text."
        )
        max_length: int = Field(
            default=None, description="max_length (int): Maximum length of the text."
        )

    config = LengthFilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.max_length = params.max_length
        self.min_length = params.min_length

    def length_filter(self, text):
        if text is None:
            return True
        length = len(text)
        if self.min_length is not None and length < self.min_length:
            return False
        if self.max_length is not None and length > self.max_length:
            return False
        return True

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        length_filter_udf = F.udf(
            lambda x: self.length_filter(x),
            BooleanType(),
        )
        df = df.withColumn(self.tag_field, length_filter_udf(F.col(self.field)))
        self.persist_tmps(df, "disk")
        res_df = return_df_by_filter_level(
            df,
            self.tag_field,
            self.mode,
        )
        return res_df

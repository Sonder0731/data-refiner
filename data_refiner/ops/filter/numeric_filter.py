from typing import Optional, Literal

from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType, DecimalType, DoubleType, FloatType, IntegerType, LongType

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

_NUMERIC_TYPES = [IntegerType(), LongType(), FloatType(), DoubleType(), DecimalType()]


@processing_operator
class NumericFilter(Filter, OperatorConstraint):
    """
    Filter rows based on a numeric column comparison. 按数值列进行过滤

    Supports comparison operators: ``<``, ``>``, ``<=``, ``>=``, ``==``.
    The target column must be a numeric type (int, long, float, double, decimal).
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly relies on the specific numeric column specified by `field`. 该算子直接依赖于由 `field` 指定的特定数值列。
* Required Data Types 要求的数据类型: The column specified by `field` must be a numeric type (`IntegerType`, `LongType`, `FloatType`, `DoubleType`, or `DecimalType`), verified by `check_column_schema` before processing. 由 `field` 指定的列必须为数值类型（`IntegerType`、`LongType`、`FloatType`、`DoubleType` 或 `DecimalType`），在处理前由 `check_column_schema` 方法进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Inspected Numeric Column 被检查数值列: This parameter designates the target column whose numeric values are compared against `threshold` using the specified `operator`. 该参数指定目标列，其数值将使用指定的 `operator` 与 `threshold` 进行比较。
* `operator` -> Comparison Operator 比较运算符: Defines the arithmetic comparison to apply. Supported values are `lt` (<), `gt` (>), `le` (<=), `ge` (>=), `eq` (==). 定义要应用的算术比较。支持的值包括 `lt` (<)、`gt` (>)、`le` (<=)、`ge` (>=)、`eq` (==)。
* `threshold` -> Comparison Threshold 比较阈值: The numeric value (integer or decimal) to compare each row's `field` value against. 用于比较每行 `field` 值的数值（整数或小数）。
* `tag_field` -> Conditional Evaluation Column 条件评估列: This parameter names the appended intermediate boolean column that holds the outcome of the numeric comparison. 该参数命名了追加的中间布尔列，用于保存数值比较的结果。
  * When the row's value satisfies the operator-threshold condition 当行值满足运算符-阈值条件时: The entry value in `tag_field` evaluates to `True`. `tag_field` 中的条目值解析为 `True`。
  * When the row's value does NOT satisfy the condition 当行值不满足条件时: The entry value in `tag_field` evaluates to `False`. `tag_field` 中的条目值解析为 `False`。
* `mode` -> Row Retention Filter 过滤行保留模式: This parameter dictates the row-slicing actions applied within `return_df_by_filter_level` based on the values in `tag_field`. 该参数决定了在 `return_df_by_filter_level` 中基于 `tag_field` 中的值所执行的行切片操作。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A user-defined function mapping to `BooleanType()` is executed via `withColumn`, appending a new intermediate boolean column named after `tag_field`. 一个映射到 `BooleanType()` 的用户自定义函数通过 `withColumn` 被执行，向当前 DataFrame 追加一个以 `tag_field` 命名的全新中间布尔列。
* Type Modifications 类型改变: The preexisting operational columns maintain their initial data types, and no inplace type transformations occur. 原有业务列保持其初始数据类型，未发生任何就地类型转换。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is sent to `return_df_by_filter_level`, where it might be structurally stripped out of the final DataFrame depending on the filtering level routine. 中间 `tag_field` 列被发送至 `return_df_by_filter_level`，在那里它可能会根据过滤级别例程从最终 DataFrame 中被结构化地剥离。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame returns the baseline structural columns identical to the input schema, whereas the temporary indicator column `tag_field` is decoupled or omitted. 最终的 DataFrame 返回与输入 Schema 相同的基线结构列，而临时指示列 `tag_field` 则被解耦或省略。
* Final Column Data Types 最终列数据类型: Every baseline column preserves its incoming operational data type without structural modifications. 每一个基线列都保留其输入的业务数据类型，未发生结构性修改。"""

    class NumericFilterParams(BaseModel):
        operator: Literal["lt", "gt", "le", "ge", "eq"] = Field(
            default="gt",
            description=(
                "Comparison operator. 比较运算符。\n"
                "  - `lt`: less than (<) 小于\n"
                "  - `gt`: greater than (>) 大于\n"
                "  - `le`: less than or equal (<=) 小于等于\n"
                "  - `ge`: greater than or equal (>=) 大于等于\n"
                "  - `eq`: equal (==) 等于"
            ),
        )
        threshold: float = Field(
            ...,
            description="The numeric threshold value to compare against. 用于比较的数值阈值（支持整数和小数）",
        )

    config = NumericFilterParams
    __slots__ = list(config.model_fields.keys())

    _OPERATOR_MAP = {
        "lt": lambda col_val, thresh: col_val < thresh,
        "gt": lambda col_val, thresh: col_val > thresh,
        "le": lambda col_val, thresh: col_val <= thresh,
        "ge": lambda col_val, thresh: col_val >= thresh,
        "eq": lambda col_val, thresh: col_val == thresh,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.operator: str = params.operator
        self.threshold: float = params.threshold

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        logger.info(
            f"Starting numeric_filter operation on field '{self.field}' "
            f"with operator '{self.operator}' and threshold '{self.threshold}'"
        )

        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, _NUMERIC_TYPES)

        compare_fn = self._OPERATOR_MAP[self.operator]

        numeric_filter_udf = F.udf(
            lambda x: bool(compare_fn(x, self.threshold)) if x is not None else True,
            BooleanType(),
        )

        df = df.withColumn(self.tag_field, numeric_filter_udf(F.col(self.field)))
        self.persist_tmps(df, "disk")
        res_df = return_df_by_filter_level(
            df,
            self.tag_field,
            self.mode,
        )

        if res_df is not None:
            recorder.record(self.output_df, res_df)

        logger.info(f"Finished numeric_filter operation")
        return res_df

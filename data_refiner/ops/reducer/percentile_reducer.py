from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType
from pydantic import BaseModel, Field

from data_refiner.ops.common.tools import resonance
from data_refiner.core import recorder
from data_refiner.core.meta_operator import Reducer, OperatorConstraint, processing_operator
from data_refiner.utils.tools import check_column_schema, check_params


@processing_operator
class PercentileReducer(Reducer, OperatorConstraint):
    """
    Calculates the percentile of a numeric column and returns a single-row DataFrame with the result. 计算数值列的百分位数，并返回包含结果的单行 DataFrame
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Target Numeric Column 目标数值列: The input DataFrame must contain this specified numeric column. 输入 DataFrame 必须包含此指定的数值列。
  * Data Type 数据类型: `IntegerType()`, `LongType()`, `FloatType()`, or `DoubleType()` 整型、长整型、单精度浮点型或双精度浮点型. This column must consist of numeric values to support the approximation histogram calculation. 该列必须由数值组成，以支持近似直方图计算。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Metric and Result Column 源指标与结果列: This parameter defines the name of the input column to evaluate, which also dictates the name of the single generated column in the output DataFrame. 该参数指定要评估的输入列名称，它同时也决定了输出 DataFrame 中生成的单个列的名称。
* `percentile` -> Rank Fraction Parameter 百分位数分数参数: This configuration parameter determines the exact mathematical percentile rank (between 0 and 1) to approximate using the internal histogram aggregation. 该配置参数决定了内部直方图聚合要近似计算的准确数学百分位数排名（介于 0 和 1 之间）。

### Schema Transformation Process Schema 转换过程
* Phase 1: Aggregate Projection and Functional Approximation 阶段 1：聚合投影与函数近似
  * The operator invokes a `.select()` transformation on the input dataset, collapsing all original structural dimensions down to a single row-expression. 算子对输入数据集调用 `.select()` 转换，将所有原始结构维度降维至单个行表达式。
  * Inside the projection, the `F.percentile_approx()` catalyst expression is evaluated, utilizing a fixed accuracy parameter of `10000` histogram buckets to compute the target value. 在投影内部，求值 `F.percentile_approx()` Catalyst 表达式，利用固定的 `10000` 个直方图桶的准确度参数来计算目标值。
* Phase 2: Metadata Renaming and Width Pruning 阶段 2：元数据重命名与宽度裁剪
  * The aggregate function result is immediately bound to the `.alias()` modifier matching the string defined by `field`. 聚合函数结果立即绑定到与 `field` 定义的字符串匹配的 `.alias()` 修饰符。
  * All input columns other than this freshly aliased calculated result are explicitly discarded, reducing the global DataFrame to a single-column, single-row summary state. 除此新起别名的计算结果之外，所有其他输入列都会被显式丢弃，从而将全局 DataFrame 缩减为单列、单行的摘要状态。

### Output Schema Final State 输出 Schema 最终态
* `field` -> Approximated Percentile Result Column 近似百分位数结果列: The sole output column containing the computed rank metric value. 包含计算出的排名指标值的唯一输出列。
  * Data Type 数据类型: `DoubleType()` 双精度浮点型. Regardless of whether the input numeric column is typed as an integer or a float, the `percentile_approx` aggregation engine natively evaluates and returns the continuous percentile marker as a double precision float field. 无论输入的数值列是整型还是浮点型，`percentile_approx` 聚合引擎原生求值并返回连续的百分位数标记作为双精度浮点型字段。"""

    class PercentileReducerParams(BaseModel):
        percentile: float = Field(
            default=0.95,
            description="The percentile to calculate. Must be between 0 and 1.",
        )

    config = PercentileReducerParams

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.percentile = params.percentile

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)

        numeric_types = [IntegerType(), LongType(), FloatType(), DoubleType()]
        check_column_schema(df, self.field, numeric_types)

        percentile_col = F.percentile_approx(F.col(self.field), self.percentile, 10000).alias(
            self.field
        )
        result_df = df.select(percentile_col)

        return result_df

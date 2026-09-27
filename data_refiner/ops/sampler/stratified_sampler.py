from loguru import logger
from pydantic import BaseModel, Field
from pyspark.sql import Window
from pyspark.sql import functions as F

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Sampler, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params


@processing_operator
class StratifiedSampler(Sampler):
    """
    Sample data by stratifying on a specified column (e.g., city), returning a fixed number of samples per group if available. 按指定列分层抽样数据，如果可用，则返回每个组的固定数量的样本
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `strata_column` -> Partition Target Column 分区目标列: The input DataFrame must contain this specified stratification key column. 输入 DataFrame 必须包含此指定的层化键列。
  * Data Type 数据类型: `Any` 任意类型. This column can hold any valid Spark SQL data type that supports grouping and partitioning (such as `StringType()` or `IntegerType()`). 该列可以包含任何支持分组和分区的有效 Spark SQL 数据类型（例如 `StringType()` 或 `IntegerType()`）。

### Argument and Column Mapping 参数与列的映射
* `strata_column` -> Stratification Key Column 层化键列: This parameter defines the name of the input column used to slice the dataset into separate categorical strata or sub-groups. 该参数指定用于将数据集切分为独立分类层或子组的输入列名称。
* `samples_per_stratum` -> Capacity Cap Parameter 容量上限参数: This configuration parameter determines the maximum integer threshold of row samples to preserve within each isolated stratum. 该配置参数决定了在每个隔离的层中要保留的行样本的最大整型阈值。

### Schema Transformation Process Schema 转换过程
* Phase 1: Window Partitioning and Sequence Generation 阶段 1：窗口分区与序列生成
  * A Spark `Window` specification is dynamically defined, partitioning the data by the value of `strata_column` and ordering rows using a non-deterministic `F.monotonically_increasing_id()` index. 一个 Spark `Window` 规范被动态定义，通过 `strata_column` 的值对数据进行分区，并使用非确定性的 `F.monotonically_increasing_id()` 索引对行进行排序。
  * The `.withColumn()` operator appends an intermediate tracking field named `"__row_num__"` into the active DataFrame schema footprint. `.withColumn()` 算子向当前 DataFrame schema 蓝图中追加一个名为 `"__row_num__"` 的中间追踪字段。
  * Data Type 数据类型: `IntegerType()` 整型. The newly appended intermediate field holds sequential 1-based indices calculated independently within each grouped partition boundary via the `F.row_number()` expression. 新追加的中间字段包含通过 `F.row_number()` 表达式在每个分组分区边界内独立计算出的、以 1 开始的顺序索引。
* Phase 2: Threshold Pruning and Temporary Purging 阶段 2：阈值裁剪与临时清除
  * A `.filter()` transformation is executed to evaluate the sequence indices, systematically discarding rows whose `"__row_num__"` values exceed the configured integer cap defined by `samples_per_stratum`. 执行 `.filter()` 转换来评估序列索引，系统性地丢弃 `"__row_num__"` 值超过由 `samples_per_stratum` 定义的配置整型上限的行。
  * The `.drop()` operator is subsequently invoked to strip the temporary `"__row_num__"` metadata tracking column out of the schema layout entirely. 随后调用 `.drop()` 算子，将临时的 `"__row_num__"` 元数据追踪列从 schema 布局中完全剥离。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the initial input DataFrame are fully retained with their original names, relative ordering, and exact data types. 初始输入 DataFrame 中存在的所有 Schema 列都将完整保留其原始名称、相对顺序和准确的数据类型。
  * No external permanent field columns are structurally injected, mutated, or deleted; the output schema structure remains identical to the input state, despite the aggregate row count reduction. 没有结构性地注入、变异或删除任何外部永久字段列；尽管总行数有所减少，但输出 schema 结构与输入状态保持完全一致。"""

    class StratifiedSamplerParams(BaseModel):
        strata_column: str = Field(..., description="The column name to stratify on.")
        samples_per_stratum: int = Field(
            default=100, description="The number of samples to take from each stratum."
        )

    config = StratifiedSamplerParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.strata_column = params.strata_column
        self.samples_per_stratum = params.samples_per_stratum

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)

        # Validate that the strata column exists
        if self.strata_column not in df.columns:
            raise ValueError(f"Column '{self.strata_column}' not found in input DataFrame")

        logger.info(
            f"Stratified sampling on column '{self.strata_column}' with {self.samples_per_stratum} samples per stratum"
        )

        window_spec = Window.partitionBy(self.strata_column).orderBy(
            F.monotonically_increasing_id()
        )
        df_with_row_num = df.withColumn("__row_num__", F.row_number().over(window_spec))

        # Filter to keep only the first `samples_per_stratum` rows per stratum
        sampled_df = df_with_row_num.filter(F.col("__row_num__") <= self.samples_per_stratum).drop(
            "__row_num__"
        )

        return sampled_df

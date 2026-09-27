import sys

from loguru import logger
from pyspark import Row
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.dependency import DeduplicatorMode
from data_refiner.core.meta_operator import Deduplicator, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class ExactDeduplicator(Deduplicator):
    """
    Deduplicate exactly same value in a field column from a whole dataset. 从整个数据集中删除字段列中完全相同的重复值
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* The input DataFrame must contain the specific column designated by `field`. 输入 DataFrame 必须包含由 `field` 指定的特定列。
* The column specified by `field` must be of `StringType` data type. 由 `field` 指定的列必须为 `StringType` 数据类型。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Column 源列: This parameter identifies the target column used to calculate the unique identifier for deduplication. 该参数确定用于计算去重唯一标识的目标列。
  * The values in `field` are passed into a SHA256 hashing function to generate a new key column named `gid`. `field` 中的值被传入 SHA256 哈希函数中，以生成名为 `gid` 的新键列。
* `mode` -> Deduplication Mode 控制模式: This parameter dictates the structural behavior of the schema transformation and the data rows retained in the final output. 该参数决定了 Schema 转换的结构行为以及最终输出中保留的数据行。
  * When `mode` is `DeduplicatorMode.DEDUP` 当 `mode` 为 `DeduplicatorMode.DEDUP` 时: The operator retains unique rows via RDD reduce operations. 算子通过 RDD reduce 操作保留唯一行。
  * When `mode` is `DeduplicatorMode.DUP` 当 `mode` 为 `DeduplicatorMode.DUP` 时: The operator appends a temporary tracking column to filter out unique records and isolate duplicates. 算子追加一个临时追踪列以过滤出唯一记录并隔离重复数据。
  * When `mode` is `DeduplicatorMode.DEDUP_WITH_DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP_WITH_DUP` 时: The operator introduces a tracking column and executes an anti-join operation to separate unique and duplicate data streams. 算子引入一个追踪列并执行反连接操作以分离唯一与重复的数据流。

### Schema Transformation Process Schema 转换过程
* Phase 1: Key Generation 阶段 1：键生成: The component applies a SHA256 UDF to the column specified by `field`, adding a temporary intermediate column named `gid` of type `StringType` to the DataFrame. 组件对 `field` 指定的列应用 SHA256 UDF，向 DataFrame 中添加一个名为 `gid` 且类型为 `StringType` 的临时中间列。
* Phase 2: Structural Branching 阶段 2：结构分支:
  * Under `DeduplicatorMode.DEDUP` 在 `DeduplicatorMode.DEDUP` 模式下: The DataFrame is converted to an RDD to perform a key-based reduction and then converted back to a DataFrame, maintaining the `gid` column. DataFrame 被转换为 RDD 以执行基于键的归约，随后转换回 DataFrame，保留 `gid` 列。
  * Under `DeduplicatorMode.DUP` 在 `DeduplicatorMode.DUP` 模式下: A temporary unique identifier column named `__id__` of type `LongType` is added via `monotonically_increasing_id()`. An anti-join operation is subsequently performed on `__id__`, which is then dropped from the final output schema. 通过 `monotonically_increasing_id()` 添加一个名为 `__id__` 且类型为 `LongType` 的临时唯一标识列。随后在 `__id__` 上执行反连接操作，该列最终会从输出 Schema 中移除。
  * Under `DeduplicatorMode.DEDUP_WITH_DUP` 在 `DeduplicatorMode.DEDUP_WITH_DUP` 模式下: Similar to the duplicate mode, `__id__` of type `LongType` is appended. An intermediate dataframe with `__id__` and an extra `__is_dup__` column of type `BooleanType` is created for anti-join mapping. 与重复模式类似，追加 `LongType` 类型的 `__id__` 列。同时创建一个包含 `__id__` 以及额外的 `BooleanType` 类型 `__is_dup__` 列的中间 DataFrame 用于反连接映射。

### Output Schema Final State 输出 Schema 最终态
* The output DataFrame retains all original input columns with their data types unaltered. 输出 DataFrame 保留所有原始输入列，且其数据类型未发生改变。
* A permanent structural addition is made to the schema: a column named `gid` of type `StringType` is included in the final output. Schema 中新增了一个永久性的结构变化：最终输出中包含一个名为 `gid` 且类型为 `StringType` 的列。"""

    def dedup_by_reduce(self, df: DataFrame):
        if self.mode == DeduplicatorMode.DEDUP:
            pair_rdd = df.rdd.map(lambda row: (row["gid"], row))
            priority_df = pair_rdd.reduceByKey(self.comparison_function).map(lambda x: x[1]).toDF()
            return priority_df
        elif self.mode == DeduplicatorMode.DUP:
            df = df.withColumn("__id__", F.monotonically_increasing_id())
            pair_rdd = (
                df.rdd.map(lambda row: (row["gid"], row))
                .reduceByKey(self.comparison_function)
                .map(lambda x: Row(__id__=x[1]["__id__"]))
            )
            self.tmp_dfs.append(pair_rdd)
            priority_df_ids = pair_rdd.toDF()
            dups_df = df.join(priority_df_ids, on=["__id__"], how="left_anti")
            return dups_df
        elif self.mode == DeduplicatorMode.DEDUP_WITH_DUP:
            df = df.withColumn("__id__", F.monotonically_increasing_id())
            pair_rdd = (
                df.rdd.map(lambda row: (row["gid"], row))
                .reduceByKey(self.comparison_function)
                .map(lambda x: x[1]["__id__"])
            )
            self.tmp_dfs.append(pair_rdd)

            priority_df_ids = pair_rdd.toDF().withColumn("__is_dup__", False)
            dups_df = df.join(priority_df_ids, on=["__id__"], how="left_anti")
            return dups_df
        else:
            logger.error(f"Invalid duplication mode: {self.mode}")
            sys.exit(1)

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        df = df.withColumn("gid", F.sha2(F.col(self.field), 256))
        res_df = self.dedup_by_reduce(df)
        return res_df

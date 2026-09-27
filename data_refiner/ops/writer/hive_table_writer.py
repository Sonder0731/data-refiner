from typing import List, Optional, Literal

from loguru import logger
from pydantic import BaseModel, Field

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Writer, processing_operator
from data_refiner.utils.tools import check_params
from data_refiner.ops.common.tools import resonance


@processing_operator
class HiveTableWriter(Writer):
    """
    Save data to a table. 将数据保存到表中
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `partition_by` -> Partition Key Columns 分区键列: If this parameter list is provided, the input DataFrame must contain all specified partition columns. 如果提供了该参数列表，输入 DataFrame 必须包含所有指定的分区列。
  * Data Type 数据类型: `Any` 任意类型. The column data types can be any valid primitive Spark SQL types (e.g., `StringType()`, `IntegerType()`, `DateType()`) capable of acting as directional keys for Hive physical path directory slicing. 列数据类型可以是任何有效的 Spark SQL 基础类型（例如 `StringType()`, `IntegerType()`, `DateType()`），能够充当 Hive 物理路径目录切分的定向键。

### Argument and Column Mapping 参数与列的映射
* `table_name` -> Target Persistence Catalog 目标持久化目录: This parameter dictates the logical database and table routing where the input layout columns are permanently mapped. 该参数决定了输入结构列永久映射的逻辑数据库和表路由。
* `partition_by` -> Structural Partitioning Column Keys 结构化分区列键: This parameter maps to a specific subset of the input schema columns to partition the physical output structure on storage blocks. 该参数映射到输入 schema 列的一个特定子集，以便在存储块上对物理输出结构进行分区。
* `dynamic_partition_overwrite` -> Runtime Partition Overwrite Strategy 运行时分区覆盖策略: When `save_mode` is `"overwrite"`, this configuration argument overrides Spark session variables to selectively replace data subsets matching the runtime evaluation of the column values. 当 `save_mode` 为 `"overwrite"` 时，该配置参数覆盖 Spark 会话变量，以选择性地替换与列值的运行时评估相匹配的数据子集。

### Schema Transformation Process Schema 转换过程
* Phase 1: Context Interception and Dynamic Overwrite Alignment 阶段 1：上下文拦截与动态覆盖对齐
  * The operator intercepts global Spark options by querying `save_mode` and `dynamic_partition_overwrite`. 算子通过查询 `save_mode` 和 `dynamic_partition_overwrite` 来拦截全局 Spark 选项。
  * If both evaluate positively, `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` is executed, changing the runtime catalog engine's transactional layout evaluation without altering the DataFrame column sequence. 如果两者均评估为真，则执行 `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")`，在不改变 DataFrame 列顺序的情况下改变运行时目录引擎的事务布局评估。
* Phase 2: Schema Integrity Verification 阶段 2：Schema 完整性校验
  * Programmatic lookup logic validates that all column text strings inside `partition_by` physically exist as native attributes inside `df.columns` to block corrupted data serialization down stream. 程序化查找逻辑验证 `partition_by` 中的所有列文本字符串在物理上作为原生属性存在于 `df.columns` 中，以阻止下游损坏的数据序列化。
* Phase 3: Catalyst Writer Instantiation and Target Saving 阶段 3：Catalyst 写入器实例化与目标保存
  * A `DataFrameWriter` instance is chain-constructed via `df.write.mode().format()`, binding configuration options including serialization layout format types, external paths, and storage parameters. 一个 `DataFrameWriter` 实例通过 `df.write.mode().format()` 链式构建，绑定了包括序列化布局格式类型、外部路径和存储参数在内的配置选项。
  * The final persistent storage execution invokes `.saveAsTable()`, tracking and pushing the active schema layer directly into the active Hive storage layer without executing runtime schema modifications or data conversions. 最终的持久化存储执行调用 `.saveAsTable()`，直接跟踪并推入当前 schema 层到当前的 Hive 存储层，而不执行运行时 schema 修改或数据转换。

### Output Schema Final State 输出 Schema 最终态
* Original Passthrough Schema 原始透传 Schema: The operator strictly returns the identical input DataFrame instance untouched, retaining the exact structural names, sequence order, nullability settings, and primitive data types. 算子严格返回未触动的相同输入 DataFrame 实例，保留准确的结构名称、排列顺序、可空性设置和基础数据类型。
  * Although data bytes are exported and written onto physical cluster storage, the active programmatic Spark DataFrame schema pipeline remains completely unaltered. 尽管数据字节被导出并写入物理集群存储，但当前的程序化 Spark DataFrame schema 管道保持完全不发生改变。"""

    class HiveTableWriterParams(BaseModel):
        table_name: str = Field(
            ...,
            description="The full name of the target Hive table, such as database.table_name.",
        )
        save_mode: Literal["append", "overwrite", "errorifexists", "ignore"] = Field(
            "append",
            description="Write modes: 'append', 'overwrite', 'errorifexists' (error if it exists), 'ignore' (ignore if it exists).",
        )

        is_external: bool = Field(
            False,
            description="Whether to create an external table. If True, you must provide external_location.",
        )
        external_location: Optional[str] = Field(
            None,
            description="The HDFS or S3 path to the external table. Valid only if is_external is True.",
        )

        format_type: Literal["parquet", "orc", "csv"] = Field(
            "parquet",
            description="For data storage format, we recommend using 'parquet' or 'orc'.",
        )
        partition_by: Optional[List[str]] = Field(
            None,
            description="A list of column names used for partitioning. For example, ['dt', 'country'].",
        )

        dynamic_partition_overwrite: bool = Field(
            False,
            description="Enable dynamic partition overwrite. If True, the 'overwrite' mode will only overwrite the partitions contained in the DataFrame, not the entire table.",
        )

        options: Optional[dict] = Field(
            None,
            description="Additional options passed to the Spark Writer, such as {'compression': 'snappy'}.",
        )

    config = HiveTableWriterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.table_name = params.table_name
        self.save_mode = params.save_mode
        self.is_external = params.is_external
        self.external_location = params.external_location
        self.format_type = params.format_type
        self.partition_by = params.partition_by
        self.dynamic_partition_overwrite = params.dynamic_partition_overwrite
        self.options = params.options

    @resonance
    def process(self, *args, **kwargs):
        spark = kwargs.get("spark")
        df = recorder.load(self.input_df)
        if self.is_external and not self.external_location:
            raise ValueError("When is_external=True, external_location cannot be empty.")

        if self.save_mode == "overwrite" and self.dynamic_partition_overwrite:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            logger.info("Enable dynamic partition overwrite mode.")
        else:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        writer = df.write.mode(self.save_mode).format(self.format_type)

        if self.partition_by:
            missing_cols = [col for col in self.partition_by if col not in df.columns]
            if missing_cols:
                raise ValueError(f"分区列缺失：DataFrame 中缺少以下列：{missing_cols}")
            writer = writer.partitionBy(*self.partition_by)

        if self.is_external:
            if not self.external_location:
                raise ValueError("When is_external=True, external_location cannot be empty.")
            writer = writer.option("path", self.external_location)
            logger.info(f"Write data to an external location:{self.external_location}")

        if self.options:
            for key, value in self.options.items():
                writer = writer.option(key, value)

        logger.info(f"Save table '{self.table_name}' by mode '{self.save_mode}'...")
        writer.saveAsTable(self.table_name)
        logger.info(f"Table [{self.table_name}] saved.")
        return df

from pydantic import BaseModel, Field
from pyspark.sql import DataFrame

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Writer, processing_operator
from data_refiner.utils.tools import check_params
from data_refiner.ops.common.tools import resonance


@processing_operator
class PathWriter(Writer):
    """
    Write data to a file system, format: "csv", "parquet", "json", "orc", "text". 将数据写入文件系统, 支持格式"csv", "parquet", "json", "orc", "text"
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `input_df` -> Input Dataset Structure 输入数据集结构: The input DataFrame serves as the source layout to be flushed onto the file system. 输入 DataFrame 充当要刷新到文件系统中的源结构布局。
  * Data Type 数据类型: `Any` 任意类型. The dataset can contain any valid Spark SQL compliant data types, provided they are supported by the target format writer (e.g., standard formats like Parquet, ORC, JSON, CSV, or Text). 该数据集可以包含任何有效的、兼容 Spark SQL 的数据类型，前提是它们受到目标格式写入器（例如 Parquet, ORC, JSON, CSV 或 Text 等标准格式）的支持。

### Argument and Column Mapping 参数与列的映射
* `path` -> Destination Storage Path 目标存储路径: This configuration parameter defines the destination URI block where the dataframe's structural layout is serialized. 该配置参数定义了序列化 DataFrame 结构布局的目标 URI 块。
* `format` -> Data Layout Format 结构布局格式: This parameter determines the underlying storage driver (such as `"parquet"`, `"orc"`, `"json"`, `"csv"`, or `"text"`) used to map columns into disk blocks. 该参数决定了将列映射到磁盘块中的底层存储驱动程序（例如 `"parquet"`, `"orc"`, `"json"`, `"csv"` 或 `"text"`）。
* `mode` -> I/O Transaction Mode 输入输出事务模式: This parameter governs the file system overwrite or append behaviors when acting on the physical storage location. 该参数在对物理存储位置进行操作时，控制文件系统的覆盖或追加行为。

### Schema Transformation Process Schema 转换过程
* Phase 1: Storage Driver Binding and Options Setup 阶段 1：存储驱动绑定与选项设置
  * The operator passes the configuration tokens to `df.write.format(format).mode(mode)`, dynamically initializing a Spark `DataFrameWriter` instance. 算子将配置标记传递给 `df.write.format(format).mode(mode)`，动态初始化一个 Spark `DataFrameWriter` 实例。
  * This phase establishes downstream storage properties but does not change the programmatic column mapping sequence or row properties within the memory layout. 此阶段建立下游存储属性，但不改变内存布局中的程序化列映射顺序或行属性。
* Phase 2: Action Execution and File Serialization 阶段 2：Action 算子执行与文件序列化
  * The `.save(path)` method is triggered, executing a Spark Action that marshals the schema architecture and flushes row datasets into file formats matching the `format` configuration. 触发 `.save(path)` 方法，执行一个 Spark Action 算子，该算子编组 schema 架构并将行数据集刷新为与 `format` 配置匹配的文件格式。
  * No internal columns are created, renamed, dropped, or cast during this execution pipeline. 在该执行管道期间，没有内部列被创建、重命名、丢弃或进行类型转换。

### Output Schema Final State 输出 Schema 最终态
* Original Passthrough Schema 原始透传 Schema: The operator strictly returns the identical input DataFrame instance untouched, retaining the exact structural names, sequence order, nullability settings, and primitive data types. 算子严格返回未触动的相同输入 DataFrame 实例，保留准确的结构名称、排列顺序、可空性设置和基础数据类型。
  * Although data records are exported and formatted onto physical disks, the continuous memory pipeline's structural footprint remains entirely unchanged. 尽管数据记录被导出并格式化到物理磁盘上，但连续内存管道的结构蓝图完全保持不变。"""

    class PathWriterParams(BaseModel):
        path: str = Field(..., description="The path to write the data to.")
        format: str = Field(..., description="The format of the data to write.")
        mode: str = Field(..., description="The mode to write the data in.")

    config = PathWriterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.path = params.path
        self.format = params.format
        self.mode = params.mode

    def _write_data(self, df: DataFrame):
        df.write.format(self.format).mode(self.mode).save(self.path)

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)
        self._write_data(df)
        return df

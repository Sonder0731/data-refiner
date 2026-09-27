from pyspark.sql import DataFrame
from pydantic import BaseModel

from data_refiner.core.meta_operator import TableReader, processing_operator
from data_refiner.ops.common.tools import resonance


@processing_operator
class HiveReader(TableReader):
    """
    This class reads table data from Hive. 从 Hive 读取表数据
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `table_name` -> Target Hive Table 目标 Hive 表: The external Hive metastore must contain the table schema pointed to by this identifier. 外部 Hive 元数据仓储必须包含此标识符所指向的表结构。
  * Data Type 数据类型: `Any` 任意类型. The input dataset originates from a physical persistent storage engine, and its schema can contain any valid Spark SQL compliant data types defined during table creation. 输入数据集源自物理持久化存储引擎，其结构可以包含建表时定义的任何有效且兼容 Spark SQL 的数据类型。

### Argument and Column Mapping 参数与列的映射
* `table_name` -> Hive Catalog Identifier Hive 目录标识符: This parameter specifies the qualified database and table name string used by the Spark session to resolve columns. 该参数指定 Spark 会话用于解析列的限定数据库和表名字符串。
  * No explicit parameter-to-column mutation occurs inside this operator; instead, the parameter maps to the entire set of existing structural fields defined within the target Hive table definition. 在该算子内部不发生显式的“参数-列”变动；相反，该参数直接映射到目标 Hive 表定义中所包含的全部现有结构化字段集合。

### Schema Transformation Process Schema 转换过程
* Phase 1: Metastore Schema Resolution 阶段 1：元数据仓储 Schema 解析
  * The operator invokes `spark.table()`, passing the identifier token specified by `table_name` to trigger a catalog lookup. 算子调用 `spark.table()`，将 `table_name` 指定的标识符标记传递给 Spark 以触发目录查找。
  * The Spark Catalyst analyzer connects to the Hive metastore to fetch the pre-existing column names and their corresponding structural types. Spark Catalyst 分析器连接到 Hive 元数据仓储，以获取预先存在的列名及其对应的结构类型。
* Phase 2: Execution Plan Binding 阶段 2：执行计划绑定
  * The retrieved schema definition is directly bound to the returned DataFrame without undergoing any internal programmatic modifications, inline filtering, or runtime column casting. 获取的 schema 定义直接绑定到返回的 DataFrame，不经历任何内部程序化修改、行内过滤或运行时列类型转换。

### Output Schema Final State 输出 Schema 最终态
* Metastore Defined Columns 元数据仓储定义列: The final schema structure is an exact reflection of the columns registered inside the external Hive system. 最终的 schema 结构完全反映了注册在外部 Hive 系统内部的列。
  * Data Type 数据类型: `StructType` 结构类型. The final output DataFrame contains all persistent fields, preserving their exact names, positions, nullability configurations, and underlying data types as specified in the Hive metadata catalog. 最终输出的 DataFrame 包含所有持久化字段，并完整保留 Hive 元数据目录中指定的准确名称、位置、可空性配置以及底层数据类型。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        spark = kwargs.get("spark")
        return spark.table(self.table_name)

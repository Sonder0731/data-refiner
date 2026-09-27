from pydantic import BaseModel, Field
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    InputOutputOperator,
    OperatorConstraint,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params
from loguru import logger


@processing_operator
class FieldTypeConverter(InputOutputOperator, OperatorConstraint):
    """
    Converts the data types of multiple specified fields in a DataFrame. 转换 DataFrame 中多个指定字段的数据类型
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field_type_mappingKeys` -> Target Source Columns 目标源列: The input DataFrame must contain the columns specified as keys in the type mapping dictionary. 输入 DataFrame 必须包含类型映射字典中作为键指定的列。
  * Data Type 数据类型: `Any` 任意类型. The initial data types of these columns can be any valid Spark SQL data types that support casting to the target data types. 这些列的初始数据类型可以是任何支持转换为目标数据类型的有效 Spark SQL 数据类型。

### Argument and Column Mapping 参数与列的映射
* `field_type_mapping` -> Type Conversion Mapping 类型转换映射: This parameter accepts a dictionary where keys represent existing column names and values represent the target Spark SQL data types as strings (e.g., `'integer'`, `'double'`, `'string'`). 该参数接收一个字典，其中键代表现有的列名，值代表字符串形式的目标 Spark SQL 数据类型（例如 `'integer'`, `'double'`, `'string'`）。

### Schema Transformation Process Schema 转换过程
* Phase 1: Schema Rebuilding and Target Type Resolution 阶段 1：Schema 重构与目标类型解析
  * The operator iterates over the fields of `original_schema` to build a `new_schema` using `StructType`. 算子遍历 `original_schema` 的字段，使用 `StructType` 构建一个 `new_schema`。
  * For each field matching a key in `field_type_mapping`, its data type is resolved via an internal map and replaced with the corresponding Spark `DataType`, while its original nullability property is preserved. 对于每个匹配 `field_type_mapping` 中键的字段，其数据类型通过内部映射进行解析，并替换为相应的 Spark `DataType`，同时保留其原始的可空性属性。
* Phase 2: Catalyst Expression Generation and Column Casting 阶段 2：Catalyst 表达式生成与列类型转换
  * A list of projection expressions is constructed by iterating through the DataFrame columns. 通过遍历 DataFrame 的列构建一个投影表达式列表。
  * For mapped columns, the `.cast()` operator is applied to convert the column to the resolved target Spark type, and `.alias()` ensures the original column name is retained; unmapped columns are passed through unchanged. 对于被映射的列，应用 `.cast()` 算子将该列转换为解析后的目标 Spark 类型，并使用 `.alias()` 确保保留原始列名；未被映射的列则保持不变直接传入。
  * The DataFrame executes a `.select()` transformation using these projection expressions to complete the type conversion without restructuring the top-level column order. DataFrame 使用这些投影表达式执行 `.select()` 转换，在不重构顶级列顺序的情况下完成类型转换。

### Output Schema Final State 输出 Schema 最终态
* Unmapped Columns 未映射列: Columns not specified in `field_type_mapping` are fully retained with their original data types and positions. 未在 `field_type_mapping` 中指定的列将完整保留其原始数据类型和位置。
* Mapped Columns 映射列: Columns specified in `field_type_mapping` retain their original names and positions, but their data types are updated. 在 `field_type_mapping` 中指定的列保留其原始名称和位置，但其数据类型会被更新。
  * Data Type 数据类型: `DataType` 目标数据类型. The final data types for these columns will strictly match the Spark SQL types resolved from the parameter values (e.g., `IntegerType()`, `DoubleType()`, `StringType()`). 这些列的最终数据类型将严格匹配从参数值解析出的 Spark SQL 类型（例如 `IntegerType()`, `DoubleType()`, `StringType()`）。"""

    class FieldTypeConverterParams(BaseModel):
        field_type_mapping: dict = Field(
            ...,
            description="A dictionary mapping field names to target Spark SQL data types as strings (e.g., {'age': 'integer', 'score': 'double'}).",
        )

    config = FieldTypeConverterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(
            self.config,
            kwargs,
        )
        self.field_type_mapping = params.field_type_mapping

    @staticmethod
    def _get_spark_type(type_str: str) -> DataType:
        """Convert string representation to actual Spark SQL DataType."""
        type_map = {
            'string': StringType(),
            'str': StringType(),
            'integer': IntegerType(),
            'int': IntegerType(),
            'long': LongType(),
            'bigint': LongType(),
            'float': FloatType(),
            'double': DoubleType(),
            'decimal': DecimalType(),
            'boolean': BooleanType(),
            'bool': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'binary': BinaryType(),
        }
        if type_str.lower() not in type_map:
            raise ValueError(f"Unsupported type: {type_str}")
        return type_map[type_str.lower()]

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)

        if not self.field_type_mapping:
            logger.info("No field type mapping provided. Returning original dataframe.")
            return df

        # Build new schema by converting specified fields
        original_schema = df.schema
        new_fields = []
        for field in original_schema.fields:
            if field.name in self.field_type_mapping:
                target_type = self._get_spark_type(self.field_type_mapping[field.name])
                new_fields.append(StructField(field.name, target_type, field.nullable))
            else:
                new_fields.append(field)
        new_schema = StructType(new_fields)

        # Use select with cast instead of mapPartitions for type conversion
        select_exprs = []
        for field_name in df.columns:
            if field_name in self.field_type_mapping:
                target_type = self._get_spark_type(self.field_type_mapping[field_name])
                select_exprs.append(df[field_name].cast(target_type).alias(field_name))
            else:
                select_exprs.append(df[field_name])

        result_df = df.select(*select_exprs)

        return result_df

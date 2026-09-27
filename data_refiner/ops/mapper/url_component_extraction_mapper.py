from furl import furl
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
from pydantic import BaseModel, Field
from typing import Literal, Set
from data_refiner.utils.tools import check_params, check_column_schema

from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    SingleInMultiOutMapper,
    OperatorConstraint,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance


def normalize_component(furl_obj: furl, component_name: str):
    match component_name:
        case "args":
            return dict(furl_obj.args)
        case "fragment":
            return str(furl_obj.fragmentstr)
        case "host":
            return furl_obj.host
        case "origin":
            return furl_obj.origin
        case "path":
            return furl_obj.pathstr
        case "port":
            return str(furl_obj.port)
        case "query":
            return furl_obj.querystr
        case "scheme":
            return furl_obj.scheme
        case _:
            return None


@processing_operator
class UrlComponentExtractionMapper(SingleInMultiOutMapper, OperatorConstraint):
    """
    Extract url components from a url field. 从 URL 字段中提取 URL 组件
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Input Column 输入列: The component requires a specific source column containing URL strings to perform the extraction logic. 组件需要一个包含 URL 字符串的特定源列来执行提取逻辑。
  * Data Type 数据类型: `StringType` 字符串类型. The schema validation explicitly checks that this column must be a string type. Schema 校验明确检查该列必须为字符串类型。

### Argument and Column Mapping 参数与列的映射
* `required_components` -> Output Struct Fields 输出结构体字段: This parameter defines a set of targeted URL components to be extracted from the source URL. 该参数定义了需要从源 URL 中提取的目标 URL 组件集合。
  * When `required_components` contains specific keys 当 `required_components` 包含特定键时: Each selected component string (e.g., `scheme`, `host`, `path`, `port`, `query`, `fragment`, `args`, `origin`) directly determines a corresponding field name in the temporary Struct column and the final expanded column names. 每个选定的组件字符串（例如 `scheme`, `host`, `path`, `port`, `query`, `fragment`, `args`, `origin`）直接决定了临时结构体列中的对应字段名以及最终炸裂展开后的列名。

### Schema Transformation Process Schema 转换过程
* Intermediate Column Creation 中间列创建: A temporary column named `"1"` is created using a User Defined Function (UDF). 使用用户自定义函数（UDF）创建了一个名为 `"1"` 的临时列。
  * The column `"1"` is of `StructType`, containing fields dynamically defined by `required_components`. 列 `"1"` 的类型为 `StructType`，其中包含由 `required_components` 动态定义的字段。
  * Each field inside the `StructType` is explicitly typed as `StringType` and is nullable. `StructType` 内部的每个字段都被明确定义为 `StringType` 类型且允许为空。
* Column Expansion and Deletion 列展开与删除: The component applies the star expansion operator (`"1.*"`) on the temporary struct column. 组件在临时结构体列上应用星号展开算子（`"1.*"`）。
  * The temporary struct container column `"1"` itself is flattened and removed from the selection. 临时结构体容器列 `"1"` 本身被扁平化展开并从选择结果中移除。
  * All fields within the struct are promoted to top-level columns in the resulting dataset. 结构体内部的所有字段都被提升为结果数据集中的顶级列。

### Output Schema Final State 输出 Schema 最终态
* Preserved Columns 保留列: All columns originally present in the input DataFrame (`original_columns`) are preserved unchanged. 输入 DataFrame 中原本存在的所有列（`original_columns`）都将原封不动地保留。
* Newly Added Columns 新增列: New columns are appended to the DataFrame based on the elements specified in `required_components`. 根据 `required_components` 中指定的元素，新列将被追加到 DataFrame 中。
  * Each newly added column will have the exact name as the component identifier (e.g., `scheme`, `host`, etc.). 每个新增列的列名将与组件标识符完全一致（例如 `scheme`, `host` 等）。
  * Data Type 数据类型: `StringType` 字符串类型. All extracted component columns are strictly typed as string format. 所有提取出的组件列都严格限制为字符串格式。"""

    class UrlComponentExtractionMapperParams(BaseModel):
        required_components: Set[
            Literal["args", "fragment", "host", "origin", "path", "port", "query", "scheme"]
        ] = Field(
            default={"scheme", "host", "path", "port", "query"},
            description="""The list of URL components to extract. Elements must be chosen from: 'scheme', 'netloc', 'path', 'params', 'query', 'fragment'. 需要提取的 URL 组件列表。元素必须从以下集合中选择：'scheme', 'netloc', 'path', 'params', 'query', 'fragment'。""",
        )

    config = UrlComponentExtractionMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: UrlComponentExtractionMapper.UrlComponentExtractionMapperParams = check_params(
            self.config, kwargs
        )
        self.required_components = params.required_components

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        def get_components(url: str):
            if url is None:
                return {required_component: None for required_component in self.required_components}
            f = furl(url)
            return {
                required_component: normalize_component(f, required_component)
                for required_component in self.required_components
            }

        df: DataFrame = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        struct_field_list = []
        for component_name in self.required_components:
            struct_field_list.append(StructField(component_name, StringType(), True))
        component_schema = StructType(struct_field_list)
        get_components_udf = F.udf(get_components, component_schema)
        original_columns = df.columns
        # check the input column schema, if the schema is complicated, you do not need to do this.
        res_df = df.withColumn("1", get_components_udf(F.col(self.field))).select(
            original_columns + ["1.*"]
        )
        return res_df

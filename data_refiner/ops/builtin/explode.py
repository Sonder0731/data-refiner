from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pydantic import BaseModel, Field

from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    SingleInMultiOutMapper,
    OperatorConstraint,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params


@processing_operator
class Explode(SingleInMultiOutMapper, OperatorConstraint):
    """
    Wrap Spark’s built-in explode function. 封装 Spark 内置的 explode 函数
    """

    CONSTRAINT = """
### 1. Input Column Constraints 输入列约束
* `field` -> Source Column 源列: This column must exist in the input DataFrame and its DataType must be either `ArrayType` or `MapType`. 该列必须存在于输入 DataFrame 中，且其数据类型必须为 `ArrayType` 或 `MapType`。
* `columns` -> Retained Columns 保留列: All existing columns in the input DataFrame will be retained during the transformation. 输入 DataFrame 中的所有现有列将在转换过程中予以保留。

### 2. Argument and Column Mapping 参数与列的映射
* `exploded_cols` -> Output Columns 输出列: This parameter specifies the names of the new columns generated after the explosion. 该参数指定炸裂后生成的新列的列名。
  * When `field` is `ArrayType` 当 `field` 为数组类型时: `exploded_cols` must contain exactly 1 element, which maps to the exploded value column. `exploded_cols` 必须恰好包含 1 个元素，映射为炸裂后的值列。
  * When `field` is `MapType` 当 `field` 为映射类型时: `exploded_cols` must contain exactly 2 elements, which map to the exploded key column and value column respectively. `exploded_cols` 必须恰好包含 2 个元素，分别映射为炸裂后的键列和值列。

### 3. Schema Transformation Process Schema 转换过程
* Initial State 初始状态: The DataFrame contains all original columns defined in `columns`. DataFrame 包含 `columns` 中定义的所有原始列。
* Transformation Step 转换步骤: The `F.explode` function is applied to the column specified by `field`. The resulting elements are aliased using the names provided in `exploded_cols` and appended to the DataFrame via a `select` operation. 对 `field` 指定的列应用 `F.explode` 函数。转换后的元素使用 `exploded_cols` 中提供的名称进行重命名，并通过 `select` 操作追加到 DataFrame 中。
* Intermediate Columns 中间列: No temporary or intermediate columns are created or deleted; the operation directly appends the final exploded columns. 没有创建或删除任何临时或中间列；该操作直接追加最终的炸裂列。

### 4. Output Schema Final State 输出 Schema 最终态
* Retained Columns 保留列: All original columns from the input DataFrame remain unchanged in both name and DataType. 输入 DataFrame 的所有原始列在名称和数据类型上均保持不变。
* New Columns Generated 新生成列: 
  * If `field` was `ArrayType` 如果 `field` 为数组类型: One new column named `exploded_cols[0]` is added. Its DataType matches the element type of the original array. 新增一个名为 `exploded_cols[0]` 的新列。其数据类型与原数组的元素类型一致。
  * If `field` was `MapType` 如果 `field` 为映射类型: Two new columns named `exploded_cols[0]` and `exploded_cols[1]` are added. Their DataTypes match the key type and value type of the original map respectively. 新增两个名为 `exploded_cols[0]` 和 `exploded_cols[1]` 的新列。它们的数据类型分别与原映射的键类型和值类型一致。"""

    class ExplodeParams(BaseModel):
        exploded_cols: List[str] = Field(
            ...,
            description="""The list of column names generated after explosion. If the input field is an Array, this list should contain 1 element (the exploded value); if it is a Map, it should contain 2 elements (corresponding to the key and value respectively). 拆解后生成的列名列表。若输入字段为 Array 类型，此列表应包含 1 个元素（即拆解后的值）；若为 Map 类型，此列表应包含 2 个元素（分别对应 key 和 value）。""",
        )

    config = ExplodeParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.exploded_cols = params.exploded_cols

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        columns: List[str] = df.columns
        res_df = df.select(*columns, F.explode(self.field).alias(*self.exploded_cols))
        return res_df

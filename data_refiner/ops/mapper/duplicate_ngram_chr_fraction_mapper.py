from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql import functions as F
from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    SingleInMultiOutMapper,
    OperatorConstraint,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from collections import defaultdict
from typing import Tuple, Dict, Iterator
from pydantic import BaseModel, Field
from data_refiner.utils.tools import check_params, check_column_schema
import pandas as pd
import numpy as np


def rolling_hash_char_ngrams(text: str, n: int):
    if len(text) < n:
        return []

    base = 257
    mod = 10**9 + 7

    hashes = []
    h = 0
    power = 1

    for i in range(n):
        h = (h * base + ord(text[i])) % mod
        if i < n - 1:
            power = (power * base) % mod

    hashes.append((0, h))

    for i in range(n, len(text)):
        h = (h - ord(text[i - n]) * power) % mod
        h = (h * base + ord(text[i])) % mod
        h = (h + mod) % mod
        hashes.append((i - n + 1, h))

    return hashes


def duplicate_ngram_chr_fraction(
    text: str,
    ngram_range: Tuple[int, int] = (5, 10),
) -> Dict[int, float]:
    if pd.isna(text):
        return {n: 0.0 for n in range(ngram_range[0], ngram_range[1] + 1)}
    L = len(text)
    if L == 0:
        return {n: 0.0 for n in range(ngram_range[0], ngram_range[1] + 1)}

    result = {}

    for n in range(ngram_range[0], ngram_range[1] + 1):

        # 1. count ngram hash
        counter = defaultdict(int)
        positions = defaultdict(list)

        hashes = rolling_hash_char_ngrams(text, n)

        for pos, h in hashes:
            counter[h] += 1
            positions[h].append(pos)

        # 2. mark duplicate chars
        is_dup = np.zeros(L, dtype=bool)
        for h, cnt in counter.items():
            if cnt > 1:
                for pos in positions[h]:
                    is_dup[pos : pos + n] = True

        # 3. compute fraction
        dup_chars = np.sum(is_dup)
        result[f"dup_{n}_ngram_chr_fraction"] = dup_chars / L

    return result


@processing_operator
class DuplicateNgramChrFractionMapper(SingleInMultiOutMapper, OperatorConstraint):
    """
    Calculate the fraction of a document's total characters that belong to any N-gram that appears more than once in that document. 计算文档中所有重复出现的 N-gram 所包含的字符数，占整篇文档总字符数的比例（比例值通常在 0 到 1 之间）。
    """

    CONSTRAINT = """
### 1. Input Column Constraints 输入列约束
* `field` -> Source Column 源列: This column must exist in the input DataFrame and its DataType must be `StringType`. 该列必须存在于输入 DataFrame 中，且其数据类型必须为 `StringType`。
* `df.schema.fields` -> Retained Columns 保留列: All existing columns in the input DataFrame will be retained and their original DataTypes will be preserved. 输入 DataFrame 中的所有现有列将被保留，且其原始数据类型将保持不变。

### 2. Argument and Column Mapping 参数与列的映射
* `ngram_range` -> Generated Feature Columns 生成特征列: This parameter defines the inclusive range of N-gram lengths used to calculate duplicate character fractions. 该参数定义了用于计算重复字符比例的 N-gram 长度的闭区间范围。
  * For each integer `n` in `range(ngram_range[0], ngram_range[1] + 1)` 对于 `range(ngram_range[0], ngram_range[1] + 1)` 中的每个整数 `n`: A specific column named `dup_n_ngram_chr_fraction` will be dynamically mapped and generated. 将动态映射并生成一个名为 `dup_n_ngram_chr_fraction` 的特定列。

### 3. Schema Transformation Process Schema 转换过程
* Initial State 初始状态: The DataFrame consists of all original input columns, with `field` validated to ensure it contains string data. DataFrame 由所有原始输入列组成，且 `field` 已通过验证以确保其包含字符串数据。
* Transformation Step 转换步骤: The operator calculates the output schema dynamically by iterating through the `ngram_range` and appending new fields via `output_schema.add`. The underlying data is processed in parallel batches using `df.mapInPandas`, where the internal `duplicate_ngram_chr_fraction` logic computes values for each row and concatenates the results along the column axis. 算子通过遍历 `ngram_range` 并通过 `output_schema.add` 追加新字段来动态计算输出 Schema。底层数据使用 `df.mapInPandas` 进行并行批处理，其中内部的 `duplicate_ngram_chr_fraction` 逻辑计算每行的值并在列轴上拼接结果。
* Intermediate Columns 中间列: Temporary Pandas DataFrames and dictionary objects are created within the streaming iterator to hold batch calculations, but no intermediate columns are written back to the persistent Spark Schema. 在流式迭代器内部创建了临时 Pandas DataFrame 和字典对象以保存批次计算结果，但没有中间列被写回到持久的 Spark Schema 中。

### 4. Output Schema Final State 输出 Schema 最终态
* Retained Columns 保留列: All original columns from the input DataFrame are preserved with their names and DataTypes unchanged. 输入 DataFrame 的所有原始列均予以保留，其名称和数据类型保持不变。
* New Columns Generated 新生成列: 
  * `dup_n_ngram_chr_fraction` -> Fraction Columns 比例列: Multiple columns will be appended, where `n` spans from the lower bound to the upper bound of `ngram_range`. The DataType for all these newly generated columns is `DoubleType`. 将追加多个列，其中 `n` 跨越 `ngram_range` 的下界到上界。所有这些新生成的列的数据类型均为 `DoubleType`。"""

    class DuplicateNgramChrFractionMapperParams(BaseModel):
        ngram_range: Tuple[int, int] = Field(default=(5, 10), description="ngram range")

    config = DuplicateNgramChrFractionMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.ngram_range = params.ngram_range

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df: DataFrame = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        output_schema = StructType(list(df.schema.fields))
        for n in range(self.ngram_range[0], self.ngram_range[1] + 1):
            new_col_name = f"dup_{n}_ngram_chr_fraction"
            if new_col_name not in output_schema.fieldNames():
                output_schema = output_schema.add(StructField(new_col_name, DoubleType(), True))

        def process_batches_stream(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for pdf_batch in iterator:
                results = []
                for text in pdf_batch[self.field]:
                    res_dict = duplicate_ngram_chr_fraction(text, self.ngram_range)
                    results.append(res_dict)
                metrics_df = pd.DataFrame(results)
                combined_df = pd.concat([pdf_batch.reset_index(drop=True), metrics_df], axis=1)
                yield combined_df

        res_df = df.mapInPandas(process_batches_stream, schema=output_schema)
        return res_df

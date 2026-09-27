from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from collections import Counter


def duplicate_paragraph_chr_fraction(text: str) -> float:
    if text is None:
        return None
    chr_len = len(text)
    if chr_len == 0:
        return 0.0

    paragraphs = text.split("\n\n")
    paragraph_counter = Counter(paragraphs)

    duplicate_chr = 0
    for t, c in paragraph_counter.items():
        if c > 1:
            duplicate_chr += len(t) * (c - 1)
    frac = duplicate_chr / chr_len
    return frac


@processing_operator
class DuplicateParagraphChrFractionMapper(SimpleMapper, OperatorConstraint):
    """
    Calculate the character fraction of duplicate paragraphs. 计算重复段落的字符数占比
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Input Column 输入列: The component requires an input DataFrame containing at least one specific column designated by the `field` parameter. 该组件要求输入 DataFrame 至少包含一个由 `field` 参数指定的特定列。
  * Data Type 数据类型: `StringType` 字符串类型. The text in this column will be segmented by double newlines to analyze duplicate paragraphs. 该列中的文本将被双换行符分割以分析重复段落。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Column 源列: This parameter specifies the name of the input column containing the raw text data to be processed. 该参数指定包含待处理原始文本数据的输入列名称。
* `output_field` -> Target Column 目标列: This parameter defines the name of the new column where the calculated duplicate paragraph character fraction will be stored. 该参数定义了用于存储计算出的重复段落字符数占比的新列名称。

### Schema Transformation Process Schema 转换过程
* Column Addition 列添加: During the execution of the `process` method, a new column specified by `output_field` is appended to the DataFrame. 在 `process` 方法的执行过程中，一个由 `output_field` 指定的新列会被追加到 DataFrame 中。
* UDF Execution UDF 执行: The user-defined function `duplicate_paragraph_chr_fraction_udf` processes the string values from `field` row by row, computes the ratio of characters in duplicate paragraphs, and writes the resulting float values into the `output_field` column. 用户自定义函数 `duplicate_paragraph_chr_fraction_udf` 逐行处理 `field` 中的字符串值，计算重复段落的字符占比，并将生成的浮点数值写入 `output_field` 列。
* Schema Retention Schema 保留: All original columns from the input DataFrame are preserved without any deletion or data type modification. 输入 DataFrame 的所有原始列均被保留，未进行任何删除或数据类型修改。

### Output Schema Final State 输出 Schema 最终态
* Retained Columns 保留列: All columns present in the input DataFrame remain unchanged in the output DataFrame. 输入 DataFrame 中存在的所有列在输出 DataFrame 中均保持不变。
* New Column 新增列: A new column named after the value of `output_field` is successfully added. 成功添加了一个以 `output_field` 的值命名的新列。
  * Data Type 数据类型: `FloatType` 浮点型. This column stores the calculated fraction of characters belonging to duplicate paragraphs as a float value between 0.0 and 1.0. 该列将属于重复段落的字符数计算占比存储为 0.0 到 1.0 之间的浮点数值。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        duplicate_paragraph_chr_fraction_udf = F.udf(duplicate_paragraph_chr_fraction, FloatType())
        res_df = df.withColumn(
            self.output_field, duplicate_paragraph_chr_fraction_udf(F.col(self.field))
        )
        return res_df

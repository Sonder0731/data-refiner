import math
from collections import Counter
from typing import Dict

from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, ArrayType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class TfIdfMapper(SimpleMapper, OperatorConstraint):
    """
    calculates the TF-IDF score for each word in a given text field. 计算语料库中某篇文档内每个词的 TF-IDF 分数
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified column. 输入 DataFrame 必须包含此指定的列。
  * Data Type 数据类型: `ArrayType(StringType())` 数组字符串类型. This column must contain an array of strings representing tokenized words. 该列必须包含代表分词结果的字符串数组。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Token Column 源分词列: This parameter defines the name of the input column containing tokenized text to calculate Term Frequency (TF). 该参数指定包含已分词文本的输入列名称，用于计算词频 (TF)。
* `output_field` -> Result Column 结果列: This parameter defines the name of the newly generated column holding the sorted TF-IDF results. 该参数指定新生成的列名称，用于存放排序后的 TF-IDF 结果。

### Schema Transformation Process Schema 转换过程
* Phase 1: Intermediate Term Frequency Calculation 阶段 1：中间词频计算
  * An intermediate key `"tf"` of type `MapType(StringType(), IntegerType())` is injected into each row dictionary via an RDD map operation. 通过 RDD map 操作，一个类型为 `MapType(StringType(), IntegerType())` 的中间键 `"tf"` 被注入到每行字典中。
  * This intermediate data structure is used to build a global Inverse Document Frequency (IDF) dictionary via a flatMap action and a broadcast variable. 该中间数据结构通过 flatMap 算子和广播变量用于构建全局逆文档频率 (IDF) 字典。
* Phase 2: TF-IDF Calculation and Schema Evolution 阶段 2：TF-IDF 计算与 Schema 演变
  * The intermediate `"tf"` key is removed from each row during the second RDD map operation. 在第二个 RDD map 操作期间，中间键 `"tf"` 从每行中被移除。
  * A new column defined by `output_field` is appended to the DataFrame. 一个由 `output_field` 指定的新列被追加到 DataFrame 中。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the input DataFrame are retained with their original data types. 输入 DataFrame 中存在的所有 Schema 列都将以其原始数据类型保留。
* `output_field` -> TF-IDF Results Column TF-IDF 结果列: The final generated output column containing scored and sorted terms. 最终生成的包含评分和排序词项的输出列。
  * Data Type 数据类型: `ArrayType(StructType([StructField('_1', StringType()), StructField('_2', IntegerType()), StructField('_3', IntegerType()), StructField('_4', DoubleType())]))` 结构数组类型. Each element in the array represents a struct containing the term string, term frequency, document frequency, and the final calculated TF-IDF score rounded to 4 decimal places. 数组中的每个元素代表一个结构体，包含词项字符串、词频、文档频率以及四舍五入保留 4 位小数的最终 TF-IDF 分数。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        spark = kwargs.get("spark")
        sc = spark.sparkContext
        field = self.field
        output_field = self.output_field

        def get_tf(row: Row):
            row_dict = row.asDict()
            row_dict["tf"] = dict(Counter(row[field])) if row[field] is not None else {}
            return row_dict

        def get_tfidf(tf: Dict):
            tfidf = []
            DF = df_dict_bc.value
            for k, v in tf["tf"].items():
                idf = math.log(doc_num / (DF[k] + 1)) + 1
                tfidf.append((k, v, DF[k], round(v * idf, 4)))
            tf.update(
                {
                    output_field: sorted(tfidf, key=lambda x: x[-1], reverse=True),
                }
            )
            tf.pop("tf")
            return Row(**tf)

        df = recorder.load(self.input_df)
        check_column_schema(df, field, ArrayType(StringType()))
        self.persist_tmps(df, "disk")
        doc_num = df.count()
        rdd_tf = df.rdd.map(get_tf)
        df_dict = dict(
            rdd_tf.flatMap(lambda x: [(k, 1) for k in x["tf"].keys()])
            .reduceByKey(lambda a, b: a + b)
            .collect()
        )
        df_dict_bc = sc.broadcast(df_dict)
        rdd_tf_idf = rdd_tf.map(get_tfidf).toDF()
        return rdd_tf_idf

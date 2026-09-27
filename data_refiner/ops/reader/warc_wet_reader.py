from io import BytesIO
from typing import List

from fastwarc.warc import ArchiveIterator
from pydantic import BaseModel, Field
from pyspark import Row
from pyspark.sql import DataFrame

from data_refiner.core.meta_operator import PathReader, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params


@processing_operator
class WarcWetReader(PathReader):
    """
    Reader for WARC/WET files from Common crawl dataset. 于读取来自通用爬虫数据集的 WARC/WET 文件
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `input_path` -> Target External Storage 目标外部存储: The files residing at the specified storage path serve as the source dataset for the Hadoop file reader. 位于指定存储路径的文件充当 Hadoop 文件读取器的源数据集。
  * Data Type 数据类型: `Any` 任意类型. The underlying raw data consists of specialized text streams or file chunks formatted as WARC/WET (Web Archive) payloads, which are chunked into text segments using a custom record delimiter. 底层原始数据由格式化为 WARC/WET（网络归档）净荷的专用文本流或文件块组成，这些数据使用自定义记录分隔符被切分为文本段。

### Argument and Column Mapping 参数与列的映射
* `types` -> Record Type Filter 记录类型过滤器: This parameter accepts a list of strings representing the explicit WARC record types to extract (e.g., `'response'`, `'conversion'`). 该参数接收一个字符串列表，指定要提取的显式 WARC 记录类型（例如 `'response'`, `'conversion'`）。
  * Record segments whose native record type names do not match any element in this list are completely excluded from downstream processing during the parsing stage. 原生记录类型名称与此列表中的任何元素均不匹配的记录段，将在解析阶段被完全排除在下游处理之外。

### Schema Transformation Process Schema 转换过程
* Phase 1: Hadoop RDD Ingestion and Custom Delimitation 阶段 1：Hadoop RDD 导入与自定义分词
  * The Spark context initializes a low-level key-value RDD via `sc.newAPIHadoopFile()`, utilizing a custom configuration dictionary `{"textinputformat.record.delimiter": "WARC/1.0"}`. Spark 上下文通过 `sc.newAPIHadoopFile()` 初始化一个低级键值对 RDD，并使用自定义配置字典 `{"textinputformat.record.delimiter": "WARC/1.0"}`。
  * This separates the input binary or text payloads into raw fragments where the key represents the line offset and the value represents the raw record string block. 这将输入的二进制或文本净荷分离为原始片段，其中键代表行偏移量，值代表原始记录字符串块。
* Phase 2: Structural Dictionary Extraction and Row Synthesis 阶段 2：结构化字典提取与行合成
  * The transformation chain applies an RDD `.map()` that runs the internal `_get_warc_text` logic over the text value component `x[-1]`. 转换链应用一个 RDD `.map()` 算子，在文本值组件 `x[-1]` 上运行内部 `_get_warc_text` 逻辑。
  * Valid fragments are parsed into an intermediate dictionary structure containing exactly two specific keys: `"url"` and `"content"`. 有效的片段被解析为一个包含且仅包含两个特定键的中间字典结构：`"url"` 和 `"content"`。
  * Unmatched, corrupt, or filtered records return empty dictionaries, which are instantly pruned from the workflow via a subsequent `.filter(lambda x: x)` operator. 不匹配、损坏或被过滤的记录将返回空字典，这些字典随后通过 `.filter(lambda x: x)` 算子立即从工作流中被修剪掉。
  * A final RDD map invocation wraps each non-empty dictionary inside a Spark `Row` object, preparing the structured collection to be converted implicitly into a final DataFrame. 最后的 RDD map 调用将每个非空字典包装在 Spark `Row` 对象中，为该结构化集合隐式转换为最终 DataFrame 做好准备。

### Output Schema Final State 输出 Schema 最终态
* Synthesized Document Columns 合成的文档列: The final DataFrame schema is strictly composed of the fields dynamically synthesized by the row dictionary constructor during runtime parsing. 最终的 DataFrame schema 严格由运行时解析期间由行字典构造函数动态合成的字段组成。
* `url` -> Target URI Column 目标统一资源标识符列: The output field holding the extracted target web address. 存放提取的目标网络地址的输出列。
  * Data Type 数据类型: `StringType()` 字符串类型. This field stores the string extracted from the `WARC-Target-URI` metadata header inside the matching record. 该字段存储从匹配记录内部的 `WARC-Target-URI` 元数据头中提取的字符串。
* `content` -> Extracted Text Payload Column 提取的文本净荷列: The output field holding the main decoded body payload. 存放解码后的主要主体净荷的输出列。
  * Data Type 数据类型: `StringType()` 字符串类型. This field stores the full text content retrieved from the record reader, completely decoded using the UTF-8 text standard. 该字段存储从记录读取器检索出的完整文本内容，完全使用 UTF-8 文本标准解码。"""

    class WarcWetReaderParams(BaseModel):
        types: List[str] = Field(
            ...,
            description="List of WARC record types to read, you could select 'response', 'conversion', etc.",
        )

    config = WarcWetReaderParams

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.types = params.types

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        spark = kwargs.get("spark")

        def _get_warc_text(raw_text):
            text = "WARC/1.0" + raw_text
            for record in ArchiveIterator(BytesIO(text.encode("utf-8"))):
                try:
                    if record.record_type.name not in self.types:
                        continue
                    return {
                        "url": record.headers.get("WARC-Target-URI"),
                        "content": record.reader.read().decode("utf-8"),
                    }
                except:
                    return {}
            return {}

        sc = spark.sparkContext
        conf = {"textinputformat.record.delimiter": "WARC/1.0"}
        rdd = sc.newAPIHadoopFile(
            self.input_path,
            "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
            "org.apache.hadoop.io.LongWritable",
            "org.apache.hadoop.io.Text",
            conf=conf,
        )
        df = rdd.map(lambda x: _get_warc_text(x[-1])).filter(lambda x: x).map(lambda x: Row(**x))
        return df

# Operator market

Processing operators currently registered in `data_refiner/ops_registry.py` are listed below.

## Built-in

| Name | Description | Details |
|:--|:--|:--|
| `explode` | Wrap Spark’s built-in explode function. 封装 Spark 内置的 explode 函数 | [explode](builtin/explode.md) |
| `sample` | Wrap Spark’s built-in sample function. 封装 Spark 内置的 sample 函数 | [sample](builtin/sample.md) |


## Reader

| Name | Description | Details |
|:--|:--|:--|
| `regular_path_reader` | Read files with a Spark DataFrameReader.<br><br>    Supported formats include csv, json, parquet, orc, text, image, and<br>    binaryFile. Use `options` to configure format-specific behavior such as<br>    CSV headers, delimiters, quoting, escaping, multiline records,<br>    encoding,<br>    schema inference, and malformed-record handling.<br><br>    使用 Spark DataFrameReader 读取文件。支持 csv、json、parquet、orc、<br>    text、image 和 binaryFile 等格式。可以通过 `options` 配置表头、分隔符、<br>    引号、转义、多行记录、编码、Schema 推断和坏记录处理等格式相关行为。 | [regular_path_reader](reader/regular_path_reader.md) |
| `hive_reader` | This class reads table data from Hive. 从 Hive 读取表数据 | [hive_reader](reader/hive_reader.md) |
| `warc_wet_reader` | Reader for WARC/WET files from Common crawl dataset. 于读取来自通用爬虫数据集的 WARC/WET 文件 | [warc_wet_reader](reader/warc_wet_reader.md) |
| `whole_text_file_reader` | Reads the whole text file as a string. Useful for reading large text files like .logs, .html files, etc. 将整个文本文件读取为字符串。适用于读取大型文本文件，例如 .log 文件、.html 文件等 | [whole_text_file_reader](reader/whole_text_file_reader.md) |


## Mapper

| Name | Description | Details |
|:--|:--|:--|
| `tfidf_mapper` | calculates the TF-IDF score for each word in a given text field. 计算语料库中某篇文档内每个词的 TF-IDF 分数 | [tfidf_mapper](mapper/tfidf_mapper.md) |
| `character_normalization_mapper` | Normalize the characters in a string. 对字符串中的字符进行规范化 | [character_normalization_mapper](mapper/character_normalization_mapper.md) |
| `chinese_traditional_to_simple_mapper` | Convert Chinese Traditional to Simple Chinese. 将繁体中文转换为简体中文 | [chinese_traditional_to_simple_mapper](mapper/chinese_traditional_to_simple_mapper.md) |
| `edit_distance_mapper` | Calculate the edit distance between two strings. 计算两个字符串之间的编辑距离 | [edit_distance_mapper](mapper/edit_distance_mapper.md) |
| `fasttext_model_mapper` | A common fasttext model mapper that takes a text field and applies a fasttext model to it. 一个常用的 fasttext 模型映射器，它接收一个文本字段并为其应用 fasttext 模型 | [fasttext_model_mapper](mapper/fasttext_model_mapper.md) |
| `filter_line_by_regex_mapper` | Filter the lines(split by newline) in a text based on a regular expression. if the line matched by the regular expression, the line will be removed. 根据正则表达式过滤文本中按换行符分割的行。如果某一行匹配该正则表达式，则移除该行 | [filter_line_by_regex_mapper](mapper/filter_line_by_regex_mapper.md) |
| `html_content_extract_mapper` | A common html content extraction ops by trafilatura library. 使用 trafilatura 库的提取 HTML 内容 | [html_content_extract_mapper](mapper/html_content_extract_mapper.md) |
| `jieba_chinese_tokenizer_mapper` | Tokenize the input text using jieba library. 使用 jieba 库对输入文本进行分词 | [jieba_chinese_tokenizer_mapper](mapper/jieba_chinese_tokenizer_mapper.md) |
| `language_identification_mapper` | This mapper uses the fasttext model to identify the language of a given text. 使用 fasttext 模型来识别给定文本的语言 | [language_identification_mapper](mapper/language_identification_mapper.md) |
| `css_extraction_mapper` | Extract CSS classes from HTML string. 从 HTML 字符串中提取 CSS | [css_extraction_mapper](mapper/css_extraction_mapper.md) |
| `mask_pii_mapper` | Mask personal identifiable information (PII) in text. 在文本中屏蔽个人身份信息 (PII) | [mask_pii_mapper](mapper/mask_pii_mapper.md) |
| `url_component_extraction_mapper` | Extract url components from a url field. 从 URL 字段中提取 URL 组件 | [url_component_extraction_mapper](mapper/url_component_extraction_mapper.md) |
| `datetime_extraction_mapper` | Extracts all datetime-related substrings from a text string using the datefinder library. 使用 datefinder 库从文本字符串中提取所有与日期时间相关的子字符串 | [datetime_extraction_mapper](mapper/datetime_extraction_mapper.md) |
| `nltk_tokenizer_mapper` | This class is used to tokenize the text data using NLTK library. 使用 NLTK 库对文本数据进行分词 | [nltk_tokenizer_mapper](mapper/nltk_tokenizer_mapper.md) |
| `unprintable_char_remove_mapper` | Remove unprintable characters from the input text. 从输入文本中移除不可打印字符 | [unprintable_char_remove_mapper](mapper/unprintable_char_remove_mapper.md) |
| `litellm_mapper` | Ask LLm for help and return json object. 请求 LLM API，并返回 JSON 的对象 | [litellm_mapper](mapper/litellm_mapper.md) |
| `text_length_mapper` | Calculate the length of a given text field and add it as a new field. 计算文本长度 | [text_length_mapper](mapper/text_length_mapper.md) |
| `character_removal_mapper` | Removes specified characters from text samples. 从文本样本中删除指定的字符 | [character_removal_mapper](mapper/character_removal_mapper.md) |
| `timestamp_mapper` | Converts a datetime string column to a Unix timestamp (seconds since epoch). 将日期时间字符串转换为 Unix 时间戳（自 Unix 纪元以来的秒数） | [timestamp_mapper](mapper/timestamp_mapper.md) |
| `url_normalization_mapper` | Normalize a URL field: lowercase scheme/host, remove default ports, sort query params, drop empty params and fragments, resolve path. 规范化 URL 字段：将协议/主机名转换为小写，移除默认端口，对查询参数进行排序，丢弃片段，解析路径 | [url_normalization_mapper](mapper/url_normalization_mapper.md) |
| `dom_element_extraction_mapper` | Extracts specified DOM elements and their subtrees from HTML content using CSS selectors or XPath, outputting the serialized HTML string of the matched elements. 使用 CSS 选择器或 XPath 从 HTML 内容中提取指定的 DOM 元素及其子树，并输出匹配元素的序列化 HTML 字符串 | [dom_element_extraction_mapper](mapper/dom_element_extraction_mapper.md) |
| `duplicate_paragraph_chr_fraction_mapper` | Calculate the character fraction of duplicate paragraphs. 计算重复段落的字符数占比 | [duplicate_paragraph_chr_fraction_mapper](mapper/duplicate_paragraph_chr_fraction_mapper.md) |
| `duplicate_line_chr_fraction_mapper` | Calculate the line fraction of duplicate paragraphs. 计算重复行的字符数占比 | [duplicate_line_chr_fraction_mapper](mapper/duplicate_line_chr_fraction_mapper.md) |
| `duplicate_ngram_chr_fraction_mapper` | Calculate the fraction of a document's total characters that belong to any N-gram that appears more than once in that document. 计算文档中所有重复出现的 N-gram 所包含的字符数，占整篇文档总字符数的比例（比例值通常在 0 到 1 之间）。 | [duplicate_ngram_chr_fraction_mapper](mapper/duplicate_ngram_chr_fraction_mapper.md) |
| `top_ngram_chr_fraction_mapper` | Calculate the fraction of the document's total characters that are accounted for by the most frequently occurring N-gram. 计算文档中出现频率最高的 N-gram 所占的字符数，占整个文档总字符数的比例 | [top_ngram_chr_fraction_mapper](mapper/top_ngram_chr_fraction_mapper.md) |


## Filter

| Name | Description | Details |
|:--|:--|:--|
| `substring_contain_filter` | Filter the dataframe by checking if a text field contains a specific substring. 通过检查文本字段是否包含特定子字符串来过滤 DataFrame | [substring_contain_filter](filter/substring_contain_filter.md) |
| `length_filter` | Filter the text by its length. 按文本长度筛选文本 | [length_filter](filter/length_filter.md) |
| `garbled_text_filter` | Identifying garbled text in a given field using a binary classification model trained by fasttext. 使用 fasttext 训练的二元分类模型判断乱码文本 | [garbled_text_filter](filter/garbled_text_filter.md) |
| `sensitive_doc_filter` | Filter the documents by the sensitive keywords. 按敏感关键词筛选文档 | [sensitive_doc_filter](filter/sensitive_doc_filter.md) |
| `trash_host_filter` | Filter the text from the trash host. 从过滤来自垃圾域名的文本 | [trash_host_filter](filter/trash_host_filter.md) |
| `null_rows_filter` | Remove rows that contain any null value across specified or all columns. 删除指定列或所有列中包含空值的行 | [null_rows_filter](filter/null_rows_filter.md) |
| `numeric_filter` | Filter rows based on a numeric column comparison. 按数值列进行过滤<br><br>    Supports comparison operators: ``<``, ``>``, ``<=``, ``>=``, ``==``.<br>    The target column must be a numeric type (int, long, float, double, decimal). | [numeric_filter](filter/numeric_filter.md) |


## Deduplicator

| Name | Description | Details |
|:--|:--|:--|
| `exact_deduplicator` | Deduplicate exactly same value in a field column from a whole dataset. 从整个数据集中删除字段列中完全相同的重复值 | [exact_deduplicator](deduplicator/exact_deduplicator.md) |
| `minhash_lsh_deduplicator` | Deduplicate records using MinHash and Locality Sensitive Hashing (LSH) algorithm. 使用 MinHash 和局部敏感哈希 (LSH) 算法对记录进行去重 | [minhash_lsh_deduplicator](deduplicator/minhash_lsh_deduplicator.md) |


## Reducer

| Name | Description | Details |
|:--|:--|:--|
| `field_count_reducer` | Count the number of occurrences of a specific field in a DataFrame. 统计 DataFrame 中特定字段出现的次数 | [field_count_reducer](reducer/field_count_reducer.md) |
| `array_flatmap_count_reducer` | Count the number of elements in an array field, it will flatten the array and count the number of elements. 计算数组字段中的元素数量，它会将数组展平并计算元素数量 | [array_flatmap_count_reducer](reducer/array_flatmap_count_reducer.md) |
| `filter_obscure_char_reducer` | Filter out the characters that appear under a certain cumsum frequency percentage of the data. 筛选出在数据中出现频率低于特定累积频率百分比的字符 | [filter_obscure_char_reducer](reducer/filter_obscure_char_reducer.md) |
| `percentile_reducer` | Calculates the percentile of a numeric column and returns a single-row DataFrame with the result. 计算数值列的百分位数，并返回包含结果的单行 DataFrame | [percentile_reducer](reducer/percentile_reducer.md) |


## Sampler

| Name | Description | Details |
|:--|:--|:--|
| `stratified_sampler` | Sample data by stratifying on a specified column (e.g., city), returning a fixed number of samples per group if available. 按指定列分层抽样数据，如果可用，则返回每个组的固定数量的样本 | [stratified_sampler](sampler/stratified_sampler.md) |


## Writer

| Name | Description | Details |
|:--|:--|:--|
| `path_writer` | Write data to a file system, format: "csv", "parquet", "json", "orc", "text". 将数据写入文件系统, 支持格式"csv", "parquet", "json", "orc", "text" | [path_writer](writer/path_writer.md) |
| `hive_table_writer` | Save data to a table. 将数据保存到表中 | [hive_table_writer](writer/hive_table_writer.md) |


## Other

| Name | Description | Details |
|:--|:--|:--|
| `spark_sql_executor` | A general-purpose operator that executes any user-provided Spark SQL query on the input DataFrame. 一个通用运算符，用于对输入的 DataFrame 执行任何用户提供的 Spark SQL | [spark_sql_executor](other/spark_sql_executor.md) |
| `field_type_converter` | Converts the data types of multiple specified fields in a DataFrame. 转换 DataFrame 中多个指定字段的数据类型 | [field_type_converter](other/field_type_converter.md) |

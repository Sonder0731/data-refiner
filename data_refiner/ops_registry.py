from typing import List, Optional, Set, Union
from data_refiner.core.registry import registry

# region: built-in operator
from data_refiner.ops.builtin.explode import Explode
from data_refiner.ops.builtin.sample import Sample

# endregion


# region: reader
from data_refiner.ops.reader.regular_path_reader import RegularPathReader
from data_refiner.ops.reader.hive_reader import HiveReader
from data_refiner.ops.reader.warc_wet_reader import WarcWetReader
from data_refiner.ops.reader.whole_text_file_reader import WholeTextFileReader
from data_refiner.ops.reducer.array_flatmap_count_reducer import (
    ArrayFlatmapCountReducer,
)

# endregion

# region: filter
from data_refiner.ops.filter.length_filter import LengthFilter
from data_refiner.ops.filter.sensitive_doc_filter import SensitiveDocFilter
from data_refiner.ops.filter.trash_host_filter import TrashHostFilter
from data_refiner.ops.filter.null_rows_filter import NullRowsFilter
from data_refiner.ops.filter.substring_contain_filter import SubstringContainFilter
from data_refiner.ops.filter.numeric_filter import NumericFilter

# endregion

# region: mapper
from data_refiner.ops.mapper.character_normalization_mapper import (
    CharacterNormalizationMapper,
)
from data_refiner.ops.mapper.chinese_traditional_to_simple_mapper import (
    ChineseTraditional2SimpleMapper,
)
from data_refiner.ops.mapper.css_extraction_mapper import CssFeaturesExtractionMapper
from data_refiner.ops.mapper.edit_distance_mapper import EditDistanceMapper
from data_refiner.ops.mapper.fasttext_model_mapper import FastTextModelMapper
from data_refiner.ops.mapper.filter_line_by_regex_mapper import FilterLineByRegexMapper
from data_refiner.ops.mapper.html_content_extract_mapper import HtmlContentExtractMapper
from data_refiner.ops.mapper.jieba_chinese_tokenizer_mapper import (
    JiebaNormalTokenizerMapper,
)
from data_refiner.ops.mapper.language_identification_mapper import (
    LanguageIdentificationMapper,
)
from data_refiner.ops.mapper.mask_pii_mapper import MaskPiiMapper
from data_refiner.ops.mapper.tfidf_mapper import TfIdfMapper
from data_refiner.ops.mapper.url_component_extraction_mapper import (
    UrlComponentExtractionMapper,
)
from data_refiner.ops.mapper.datetime_extraction_mapper import DatetimeExtractionMapper
from data_refiner.ops.mapper.dom_element_extraction_mapper import DomElementExtractionMapper
from data_refiner.ops.mapper.nltk_tokenizer_mapper import NltkTokenizerMapper
from data_refiner.ops.mapper.unprintable_char_remove_mapper import UnprintableCharRemoveMapper
from data_refiner.ops.mapper.litellm_mapper import LiteLLMMapper
from data_refiner.ops.mapper.text_length_mapper import TextLengthMapper
from data_refiner.ops.mapper.character_removal_mapper import CharacterRemovalMapper
from data_refiner.ops.mapper.timestamp_mapper import TimestampMapper
from data_refiner.ops.mapper.url_normalization_mapper import UrlNormalizationMapper
from data_refiner.ops.mapper.duplicate_paragraph_chr_fraction_mapper import (
    DuplicateParagraphChrFractionMapper,
)
from data_refiner.ops.mapper.duplicate_line_chr_fraction_mapper import (
    DuplicateLineChrFractionMapper,
)
from data_refiner.ops.mapper.duplicate_ngram_chr_fraction_mapper import (
    DuplicateNgramChrFractionMapper,
)
from data_refiner.ops.mapper.top_ngram_chr_fraction_mapper import TopNgramChrFractionMapper

# endregion

# region: deduplicator
from data_refiner.ops.deduplicator.exact_deduplicator import ExactDeduplicator
from data_refiner.ops.deduplicator.minhash_lsh_deduplicator import (
    MinhashLSHDeduplicator,
)
from data_refiner.ops.filter.garbled_text_filter import GarbledTextFilter

# endregion

# region: reducer
from data_refiner.ops.reducer.field_count_reducer import FieldCountReducer
from data_refiner.ops.reducer.filter_obscure_char_reducer import (
    FilterObscureCharReducer,
)
from data_refiner.ops.reducer.percentile_reducer import PercentileReducer

# endregion

# region: sampler
from data_refiner.ops.sampler.stratified_sampler import StratifiedSampler
from data_refiner.ops.writer.hive_table_writer import HiveTableWriter

# endregion

# region: writer
from data_refiner.ops.writer.path_writer import PathWriter

# endregion

# region: other
from data_refiner.ops.other.spark_sql_executor import SparkSqlExecutor
from data_refiner.ops.other.field_type_converter import FieldTypeConverter

# endregion

OPS = [
    # built-in
    Explode,
    Sample,
    # reader
    RegularPathReader,
    HiveReader,
    WarcWetReader,
    WholeTextFileReader,
    # mapper
    TfIdfMapper,
    CharacterNormalizationMapper,
    ChineseTraditional2SimpleMapper,
    EditDistanceMapper,
    FastTextModelMapper,
    FilterLineByRegexMapper,
    HtmlContentExtractMapper,
    JiebaNormalTokenizerMapper,
    LanguageIdentificationMapper,
    CssFeaturesExtractionMapper,
    MaskPiiMapper,
    UrlComponentExtractionMapper,
    DatetimeExtractionMapper,
    NltkTokenizerMapper,
    UnprintableCharRemoveMapper,
    LiteLLMMapper,
    TextLengthMapper,
    CharacterRemovalMapper,
    TimestampMapper,
    UrlNormalizationMapper,
    DomElementExtractionMapper,
    DuplicateParagraphChrFractionMapper,
    DuplicateLineChrFractionMapper,
    DuplicateNgramChrFractionMapper,
    TopNgramChrFractionMapper,
    # filter
    SubstringContainFilter,
    LengthFilter,
    GarbledTextFilter,
    SensitiveDocFilter,
    TrashHostFilter,
    NullRowsFilter,
    NumericFilter,
    # reducer
    FieldCountReducer,
    ArrayFlatmapCountReducer,
    FilterObscureCharReducer,
    PercentileReducer,
    # deduplicator
    ExactDeduplicator,
    MinhashLSHDeduplicator,
    # sampler
    StratifiedSampler,
    # writer
    PathWriter,
    HiveTableWriter,
    # other
    SparkSqlExecutor,
    FieldTypeConverter,
]

mnf = lambda x: x.split(".")[-1]
OPS_MAPPING = {mnf(op.__module__): op for op in OPS}


def register_ops(
    op_names: Optional[Union[List[str], Set[str]]] = None,
    additional_ops_mapping: Optional[dict] = None,
):
    if additional_ops_mapping:
        if OPS_MAPPING.keys() & additional_ops_mapping.keys():
            raise ValueError(
                f"Duplicate operator name: {OPS_MAPPING.keys() & additional_ops_mapping.keys()}"
            )
        OPS_MAPPING.update(additional_ops_mapping)
    if op_names:
        for name in op_names:
            if name in OPS_MAPPING:
                registry.register(name, OPS_MAPPING[name])
            elif registry.get_op(name):
                continue
            else:
                raise ValueError(f"{name} is not a valid op name")
    else:
        for name, op in OPS_MAPPING.items():
            registry.register(name, op)

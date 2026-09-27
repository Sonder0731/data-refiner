import os
from pathlib import Path

from tests.pipeline.conftest import TEST_ROOT


def register_env_vars(var_name, var_value):
    os.environ[var_name] = str(var_value) if isinstance(var_value, Path) else var_value


TEST_PIPELINE_CFG_PATH = TEST_ROOT.joinpath("pipeline_cfg_files")


# html structure similarity detection
HTML_STRUCTURE_SIMILARITY_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath(
    "html_structure_similarity_detection.yaml"
)
register_env_vars(
    "HTML_STRUCTURE_SIMILARITY_PIPELINE_PATH", HTML_STRUCTURE_SIMILARITY_PIPELINE_PATH
)


# chinese traditional2simple converter
CHINESE_TRADITIONAL2SIMPLE_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath(
    "chinese_traditional2simple.yaml"
)
register_env_vars(
    "CHINESE_TRADITIONAL2SIMPLE_PIPELINE_PATH", CHINESE_TRADITIONAL2SIMPLE_PIPELINE_PATH
)

# character normalization
CHARACTER_NORMALIZATION_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath(
    "character_normalization.yaml"
)
register_env_vars("CHARACTER_NORMALIZATION_PIPELINE_PATH", CHARACTER_NORMALIZATION_PIPELINE_PATH)

# edit distance
EDIT_DISTANCE_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath("edit_distance.yaml")
register_env_vars("EDIT_DISTANCE_PIPELINE_PATH", EDIT_DISTANCE_PIPELINE_PATH)

# filter by line regex
FILTER_BY_LINE_REGEX_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath("filter_by_line_regex.yaml")
register_env_vars("FILTER_BY_LINE_REGEX_PIPELINE_PATH", FILTER_BY_LINE_REGEX_PIPELINE_PATH)

# html content extraction
HTML_CONTENT_EXTRACTION_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath(
    "html_content_extraction.yaml"
)
register_env_vars("HTML_CONTENT_EXTRACTION_PIPELINE_PATH", HTML_CONTENT_EXTRACTION_PIPELINE_PATH)

# jieba tokens
JIEBA_NORMAL_TOKENIZER_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath("jieba_tokenizer.yaml")
register_env_vars("JIEBA_NORMAL_TOKENIZER_PIPELINE_PATH", JIEBA_NORMAL_TOKENIZER_PIPELINE_PATH)

# language identification
LANGUAGE_IDENTIFICATION_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath(
    "language_identification.yaml"
)
register_env_vars("LANGUAGE_IDENTIFICATION_PIPELINE_PATH", LANGUAGE_IDENTIFICATION_PIPELINE_PATH)

# mask pii
MASK_PII_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath("mask_pii.yaml")
register_env_vars("MASK_PII_PIPELINE_PATH", MASK_PII_PIPELINE_PATH)

# test temp view
TEST_TEMP_VIEW_PIPELINE_PATH = TEST_PIPELINE_CFG_PATH.joinpath("test_temp_view.yaml")
register_env_vars("TEST_TEMP_VIEW_PIPELINE_PATH", TEST_TEMP_VIEW_PIPELINE_PATH)

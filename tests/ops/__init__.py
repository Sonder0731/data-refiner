import os
from pathlib import Path

TEST_ROOT = Path(__file__).parent.resolve()
TEST_OPS_DATA_PATH = TEST_ROOT.joinpath("test_data")

# html content extract mapper test data path
HTML_CONTENT_EXTRACT_MAPPER_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath(
    "mapper/html_content_extract_mapper"
)
os.environ["HTML_CONTENT_EXTRACT_MAPPER_TESTDATA_PATH"] = (
    HTML_CONTENT_EXTRACT_MAPPER_TESTDATA_PATH.as_uri()
)

# additional sensitive keywords
ADDITIONAL_SENSITIVE_KEYWORDS = TEST_OPS_DATA_PATH.joinpath(
    "filter/sensitive_doc_filter/keywords.json"
)
os.environ["ADDITIONAL_SENSITIVE_KEYWORDS"] = (
    ADDITIONAL_SENSITIVE_KEYWORDS.as_uri()
)

# additional trash hosts
ADDITIONAL_TRASH_HOSTS = TEST_OPS_DATA_PATH.joinpath(
    "filter/trash_host_filter/trash_host.json"
)
os.environ["ADDITIONAL_TRASH_HOSTS"] = (
    ADDITIONAL_TRASH_HOSTS.as_uri()
)
# file path reader test data path
PATH_READER_CSV_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/csv")
PATH_READER_JSON_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/json")
PATH_READER_IMAGE_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/image")
PATH_READER_AUDIO_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/audio")

PATH_READER_CC_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/cc")
PATH_READER_WARC_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/cc/warc")
PATH_READER_WET_TESTDATA_PATH = TEST_OPS_DATA_PATH.joinpath("reader/cc/wet")

os.environ["PATH_READER_CSV_TESTDATA_PATH"] = PATH_READER_CSV_TESTDATA_PATH.as_uri()
os.environ["PATH_READER_JSON_TESTDATA_PATH"] = PATH_READER_JSON_TESTDATA_PATH.as_uri()
os.environ["PATH_READER_CC_TESTDATA_PATH"] = PATH_READER_CC_TESTDATA_PATH.as_uri() + "/**"
os.environ["PATH_READER_WARC_TESTDATA_PATH"] = PATH_READER_WARC_TESTDATA_PATH.as_uri()
os.environ["PATH_READER_WET_TESTDATA_PATH"] = PATH_READER_WET_TESTDATA_PATH.as_uri()
os.environ["PATH_READER_IMAGE_TESTDATA_PATH"] = PATH_READER_IMAGE_TESTDATA_PATH.as_uri()
os.environ["PATH_READER_AUDIO_TESTDATA_PATH"] = PATH_READER_AUDIO_TESTDATA_PATH.as_uri()

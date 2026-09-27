from pathlib import Path

from data_refiner.utils.tools import ensure_path_exists
from loguru import logger


def print_tree(path: Path, max_depth: int = 2, prefix: str = "", current_depth: int = 0):
    if not path.exists():
        logger.info(f"{path} 不存在")
        return

    if max_depth is not None and current_depth >= max_depth:
        return

    items = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name))

    for index, item in enumerate(items):
        connector = "└── " if index == len(items) - 1 else "├── "
        logger.info(prefix + connector + item.name)

        if item.is_dir():
            if max_depth is None or current_depth + 1 < max_depth:
                extension = "    " if index == len(items) - 1 else "│   "
                print_tree(item, max_depth, prefix + extension, current_depth + 1)


class LocalPath:
    import data_refiner

    @staticmethod
    def repo_root() -> Path:
        # deal with add .whl to --py-files lead to wrong repo_root
        repo_root = Path(LocalPath.data_refiner.__file__).parent.parent
        if repo_root.is_file():
            return repo_root.parent
        return repo_root

    @staticmethod
    def project_root() -> Path:
        return LocalPath.repo_root().joinpath("data_refiner")

    @staticmethod
    @ensure_path_exists
    def operator_root() -> Path:
        return LocalPath.project_root().joinpath("ops")

    @staticmethod
    @ensure_path_exists
    def runtime_resources_root() -> Path:
        return Path.cwd().joinpath("data-refiner-runtime-resources")

    @staticmethod
    @ensure_path_exists
    def model_root() -> Path:
        return LocalPath.runtime_resources_root().joinpath("models")

    @staticmethod
    @ensure_path_exists
    def data_root() -> Path:
        return LocalPath.runtime_resources_root().joinpath("data")

    @staticmethod
    @ensure_path_exists
    def jar_root() -> Path:
        return LocalPath.runtime_resources_root().joinpath("jars")

    @staticmethod
    @ensure_path_exists
    def temp_root() -> Path:
        return LocalPath.runtime_resources_root().joinpath("temp")

    @staticmethod
    @ensure_path_exists
    def mapper_root() -> Path:
        return LocalPath.operator_root().joinpath("mapper")

    @staticmethod
    @ensure_path_exists
    def filter_root() -> Path:
        return LocalPath.operator_root().joinpath("filter")

    @staticmethod
    @ensure_path_exists
    def deduplicator_root() -> Path:
        return LocalPath.operator_root().joinpath("deduplicator")

    @staticmethod
    @ensure_path_exists
    def reducer_root() -> Path:
        return LocalPath.operator_root().joinpath("reducer")

    @staticmethod
    @ensure_path_exists
    def sampler_root() -> Path:
        return LocalPath.operator_root().joinpath("sampler")

    @staticmethod
    @ensure_path_exists
    def reader_root() -> Path:
        return LocalPath.operator_root().joinpath("reader")

    @staticmethod
    @ensure_path_exists
    def writer_root() -> Path:
        return LocalPath.operator_root().joinpath("writer")

    @staticmethod
    @ensure_path_exists
    def test_root() -> Path:
        return LocalPath.repo_root().joinpath("tests")

    @staticmethod
    @ensure_path_exists
    def test_operator_root() -> Path:
        return LocalPath.test_root().joinpath("ops")

    @staticmethod
    @ensure_path_exists
    def test_pipeline_root() -> Path:
        return LocalPath.test_root().joinpath("pipeline")

    @staticmethod
    @ensure_path_exists
    def test_pipeline_conf_root() -> Path:
        return LocalPath.test_pipeline_root().joinpath("pipeline_cfg_files")

    @staticmethod
    @ensure_path_exists
    def test_mapper_root() -> Path:
        return LocalPath.test_operator_root().joinpath("mapper")

    @staticmethod
    @ensure_path_exists
    def test_filter_root() -> Path:
        return LocalPath.test_operator_root().joinpath("filter")

    @staticmethod
    @ensure_path_exists
    def test_deduplicator_root() -> Path:
        return LocalPath.test_operator_root().joinpath("deduplicator")

    @staticmethod
    @ensure_path_exists
    def test_reducer_root() -> Path:
        return LocalPath.test_operator_root().joinpath("reducer")

    @staticmethod
    @ensure_path_exists
    def test_sampler_root() -> Path:
        return LocalPath.test_operator_root().joinpath("sampler")

    @staticmethod
    @ensure_path_exists
    def test_reader_root() -> Path:
        return LocalPath.test_operator_root().joinpath("reader")

    @staticmethod
    @ensure_path_exists
    def test_writer_root() -> Path:
        return LocalPath.test_operator_root().joinpath("writer")

    @staticmethod
    @ensure_path_exists
    def operator_docs_root() -> Path:
        return LocalPath.repo_root().joinpath("docs/operator")

    @staticmethod
    @ensure_path_exists
    def builtin_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("builtin")

    @staticmethod
    @ensure_path_exists
    def deduplicator_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("deduplicator")

    @staticmethod
    @ensure_path_exists
    def filter_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("filter")

    @staticmethod
    @ensure_path_exists
    def mapper_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("mapper")

    @staticmethod
    @ensure_path_exists
    def meta_operator_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("meta_operator")

    @staticmethod
    @ensure_path_exists
    def other_operator_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("other")

    @staticmethod
    @ensure_path_exists
    def reader_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("reader")

    @staticmethod
    @ensure_path_exists
    def reducer_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("reducer")

    @staticmethod
    @ensure_path_exists
    def sampler_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("sampler")

    @staticmethod
    @ensure_path_exists
    def writer_doc_root() -> Path:
        return LocalPath.operator_docs_root().joinpath("writer")


class ClusterPath:
    @staticmethod
    def runtime_resources_root() -> Path:
        """
        return data-refiner-runtime-resources/
        """
        return Path("data-refiner-runtime-resources")

    @staticmethod
    def model_root() -> Path:
        """
        return data-refiner-runtime-resources/models
        """
        return ClusterPath.runtime_resources_root().joinpath("models")

    @staticmethod
    def data_root() -> Path:
        """
        return data-refiner-runtime-resources/data
        """
        return ClusterPath.runtime_resources_root().joinpath("data")

    @staticmethod
    def jar_root() -> Path:
        """
        return data-refiner-runtime-resources/jars
        """
        return ClusterPath.runtime_resources_root().joinpath("jars")

    @staticmethod
    def temp_root() -> Path:
        """
        return data-refiner-runtime-resources/temp
        """
        return ClusterPath.runtime_resources_root().joinpath("temp")

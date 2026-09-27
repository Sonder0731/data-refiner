import os
import shutil
import tempfile
from enum import Enum
from pathlib import Path
from typing import List, Tuple

from loguru import logger
from tqdm import tqdm
from wget import download

from data_refiner.utils.path_set import LocalPath
from data_refiner.utils.tools import tar_gz_extractor

# region: path variable
REPO_ROOT = LocalPath.repo_root()
PROJECT_ROOT = LocalPath.project_root()
RUNTIME_RESOURCES_ROOT = LocalPath.runtime_resources_root()
MODEL_ROOT = LocalPath.model_root()
DATA_ROOT = LocalPath.data_root()
JAR_ROOT = LocalPath.jar_root()
TEMP_ROOT = LocalPath.temp_root()


class ResourceType(Enum):
    DATA = "data"
    MODEL = "model"
    JAR = "jar"


class Resource:
    def __init__(
        self,
        resource_type: ResourceType,
        env_var_name: str,
        resources: List[Tuple[str, str]],
    ):
        """
        :param resource_type
        :param env_var_name
        :param resources: [(download_url,file_name)]
        """
        self.resource_type = resource_type
        self.env_var_name = env_var_name
        self.resources = resources
        self.export()

    def normalize_path(self, file_name) -> Path:
        if self.resource_type == ResourceType.DATA:
            path = DATA_ROOT.joinpath(file_name)
        elif self.resource_type == ResourceType.MODEL:
            path = MODEL_ROOT.joinpath(file_name)
        elif self.resource_type == ResourceType.JAR:
            path = JAR_ROOT.joinpath(file_name)
        else:
            raise ValueError("Unsupported dependency type")
        return path

    def export(self):
        paths = []
        for download_url, file_name in self.resources:
            file_path = self.normalize_path(file_name)
            file_path_string = WgetDownloader.download(download_url, file_path)
            paths.append(file_path_string)
        os.environ[self.env_var_name] = ";".join(paths)


class CompressedResource:
    def __init__(self, resource_type, env_var_name: str, resources: List[Tuple[str, str]]):
        """
        :param env_var_name
        :param resources: [download_url, sub_path)]
        """
        self.resource_type = resource_type
        self.env_var_name = env_var_name
        self.resources = resources
        self.uncompressed_path = str(self.determine_uncompressed_path())
        self.export()

    def determine_uncompressed_path(self) -> Path:
        if self.resource_type == ResourceType.DATA:
            path = DATA_ROOT
        elif self.resource_type == ResourceType.MODEL:
            path = MODEL_ROOT
        else:
            raise ValueError("Unsupported dependency type")
        return path

    def export(self):
        paths = []
        for download_url, sub_path in self.resources:
            real_resource_path = Path(self.uncompressed_path).joinpath(Path(sub_path).name)
            if Path(real_resource_path).exists():
                paths.append(str(real_resource_path))
                break
            resource_name = Path(download_url).name
            with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as temp_dir:
                resource_temp_path = Path(temp_dir).joinpath(resource_name)
                WgetDownloader.download(download_url, resource_temp_path)
                tar_gz_extractor(resource_temp_path, temp_dir)
                shutil.move(Path(temp_dir).joinpath(sub_path), Path(self.uncompressed_path))
            paths.append(str(real_resource_path))
        os.environ[self.env_var_name] = ";".join(paths)


class WgetDownloader:
    @staticmethod
    def download(resource_url, resource_path) -> str:
        if resource_path.exists():
            return str(resource_path)
        pbar = tqdm(
            total=0,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Resource downloading {resource_url}",
            ascii=True,
        )
        pbar.clear()

        def tqdm_bar(current, total, width=80):
            if pbar.total != total:
                pbar.reset(total=total)
            pbar.n = current
            pbar.refresh()

        logger.info(f"Downloading {resource_url} to {resource_path}")
        download(resource_url, out=str(resource_path), bar=tqdm_bar)
        if resource_path.exists():
            return str(resource_path)
        else:
            logger.warning(
                f"Download failed: {resource_url}, please download manually and move it in {resource_path.resolve()}"
            )
            return None

    @staticmethod
    def download_and_unzip(resource_url: str, resource_path: str):
        """
        下载一个文件到临时路径，执行 func(temp_path)，并在结束后自动删除文件。

        Args:
            resource_url: 下载链接
            func: 接受 Path 类型参数的函数
        Returns:
            func 执行结果，或 None（如果下载失败）
        """
        resource_name = Path(resource_url).name
        with tempfile.TemporaryDirectory(dir=os.environ["TEMP_ROOT"]) as temp_dir:
            resource_temp_path = Path(temp_dir).joinpath(resource_name)
            WgetDownloader.download(resource_url, resource_temp_path)
            tar_gz_extractor(resource_temp_path, resource_path)

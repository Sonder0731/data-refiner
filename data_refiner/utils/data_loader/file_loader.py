from pathlib import Path

import orjson
from json_repair import repair_json
from loguru import logger

from data_refiner.utils.tools import get_all_files_recursive


class FileLoader:
    def __init__(self, path):
        self.path = Path(path) if not isinstance(path, Path) else path

    def get_files(self):
        if self.path.is_file():
            return [self.path]
        elif self.path.is_dir():
            return get_all_files_recursive(self.path)
        else:
            if str(self.path).count(";") > 0:
                file_paths = str(self.path).split(";")
                return [
                    sub_path
                    for file_path in file_paths
                    for sub_path in get_all_files_recursive(Path(file_path).resolve())
                ]
            else:
                raise ValueError(f"Invalid path: {self.path}")

    def match_file_extension(self):
        raise NotImplementedError()

    def get_data(self):
        raise NotImplementedError()


class JsonLoader(FileLoader):
    def match_file_extension(self):
        files = self.get_files()
        return list(filter(lambda x: x.suffix == ".json" or x.suffix == ".jsonl", files))

    def get_data(self):
        data = []
        files = self.match_file_extension()
        for file_ in files:
            with open(file_, "r", encoding="utf-8") as fr:
                for line in fr.readlines():
                    try:
                        data.append(orjson.loads(line))
                    except orjson.JSONDecodeError:
                        try:
                            data.append(repair_json(line, return_objects=True))
                        except:
                            logger.warning(f"Failed to decode line {line}")
        return data


class TextLoader(FileLoader):
    def match_file_extension(self):
        files = self.get_files()
        return list(filter(lambda x: x.suffix == ".txt" or not x.suffix, files))

    def get_data(self):
        text = ""
        files = self.match_file_extension()
        for file_ in files:
            with open(file_, "r", encoding="utf-8") as fr:
                text += fr.read()
        return text

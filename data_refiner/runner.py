import sys
from collections import Counter
from typing import Dict, List, Iterable, Optional

import yaml
from loguru import logger
from pyspark.sql import SparkSession

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Operator
from data_refiner.core.registry import registry
from data_refiner.ops_registry import register_ops
from data_refiner.utils.path_set import LocalPath


class Runner:
    def __init__(self, config=None):
        self.config = config

    @classmethod
    def from_yaml(cls, path):
        config = yaml.safe_load(open(path, "r", encoding="utf-8"))
        return cls(config)

    @classmethod
    def from_dict(cls, config: dict):
        return cls(config)

    def _load_ops(self, additional_ops_mapping: Dict):
        op_names = [v["op_name"] for k, v in self.config.items() if v["op_name"]]
        register_ops(set(op_names), additional_ops_mapping=additional_ops_mapping)

    def static_validation(self, additional_ops_mapping: Dict):
        self._load_ops(additional_ops_mapping)
        temp_view_names = []
        for node_name, op_config in self.config.items():
            op_name = op_config.get("op_name")
            cls: type[Operator] | None = registry.get_op(op_name)
            try:
                op = cls(**op_config)
            except Exception as exc:
                raise ValueError(f"Failed to validate {op_name}, exception: {exc}") from exc
            if op.temp_view_name is not None and op.temp_view_name in temp_view_names:
                raise ValueError(f"Duplicate temp view name {op.temp_view_name}")
            temp_view_names.append(op.temp_view_name)

    def opt_cache_flag(self):
        input_dfs = [
            df_name
            for df_name, count in Counter(
                [
                    op_params["input_df"]
                    for _, op_params in self.config.items()
                    if op_params.get("input_df")
                ]
            ).items()
            if count > 1
        ]

        for op_name, op_params in self.config.items():
            if op_params.get("input_df") in input_dfs:
                op_params["cache"] = "disk"

    def load_procedure(self, additional_ops_mapping: Dict) -> List[Operator]:
        self.static_validation(additional_ops_mapping)
        self.opt_cache_flag()
        procedure = []
        for op_alias, op_params in self.config.items():
            op_name = op_params["op_name"]
            op_instance = registry.get_op(op_name)(**op_params)
            procedure.append(op_instance)
        return procedure

    def run(
        self,
        spark: SparkSession,
        pipeline: Optional[Iterable[Operator]] = None,
        additional_ops_mapping: Dict = None,
    ):
        procedure = pipeline or self.load_procedure(additional_ops_mapping)
        df = None
        for op_instance in procedure:
            logger.info(f"Running step {op_instance.__class__.__name__}")
            df = op_instance.run(spark=spark)
        registry.clear()
        recorder.clear()
        return df

    def run_locally(
        self,
        spark: SparkSession,
        pipeline: Optional[Iterable[Operator]] = None,
        additional_ops_mapping: Dict = None,
    ):
        procedure = pipeline or self.load_procedure(additional_ops_mapping)
        for op in procedure:
            if hasattr(op, "input_path"):
                real_path = LocalPath.repo_root().joinpath(op.input_path)
                if not sys.platform.startswith("win"):
                    real_path = real_path.as_uri()
                op.input_path = str(real_path)

        df = None
        for op_instance in procedure:
            logger.info(f"Running step {op_instance.__class__.__name__}")
            df = op_instance.run(spark=spark)
        registry.clear()
        recorder.clear()
        return df

from loguru import logger
import sys

__all__ = [
    "Runner",
    "registry",
    "resonance",
    "OperatorConstraint",
    "OperatorReference",
    "OperatorExample",
    "Operator",
    "InputOutputOperator",
    "Reader",
    "PathReader",
    "TableReader",
    "Writer",
    "Reducer",
    "SimpleMapper",
    "MultiInSingleOutMapper",
    "SingleInMultiOutMapper",
    "MultiInMultiOutMapper",
    "Filter",
    "Deduplicator",
    "Sampler",
]

# region: init logger
logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="[<level>{level}</level>] <level>{message}</level>",
)
# endregion
from data_refiner.runner import Runner

from data_refiner.core.registry import registry
from data_refiner.ops.common.tools import resonance
from data_refiner.core.meta_operator import (
    OperatorConstraint,
    OperatorReference,
    OperatorExample,
    Operator,
    InputOutputOperator,
    Reader,
    PathReader,
    TableReader,
    Writer,
    Reducer,
    SimpleMapper,
    MultiInSingleOutMapper,
    SingleInMultiOutMapper,
    MultiInMultiOutMapper,
    Filter,
    Deduplicator,
    Sampler,
)

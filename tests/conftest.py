import pytest


def pytest_addoption(parser):
    """注册 --cluster 和 --local 参数"""
    parser.addoption("--cluster", action="store_true", help="Run in cluster mode")
    parser.addoption("--local", action="store_true", help="Run in local mode")

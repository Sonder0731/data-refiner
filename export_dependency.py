import subprocess
import sys
from pathlib import Path
from data_refiner.utils.path_set import LocalPath

CONDA_ENV = "data_refiner_env"
CONDA_PACK = Path(sys.executable).with_name("conda-pack")
REQUIREMENTS = LocalPath.repo_root() / "requirements.txt"


def main():
    export = subprocess.run(
        [
            "uv",
            "export",
            "--locked",
            "--no-dev",
            "--no-emit-project",
            "--no-emit-package",
            "packaging",
            "--no-emit-package",
            "setuptools",
            "--no-hashes",
            "--format",
            "requirements.txt",
            "--output-file",
            str(REQUIREMENTS),
        ],
        capture_output=True,
        text=True,
        errors="replace",
        cwd=str(LocalPath.repo_root()),
        timeout=300,
        check=False,
    )

    return {
        "exit_code": export.returncode,
        "stdout": export.stdout,
        "stderr": export.stderr,
    }

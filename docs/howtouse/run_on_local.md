# Running Data-refiner in Local Mode
This guide describes how to run Data Refiner locally, either directly from the source code or by installing the data-refiner Python package.

⚠️ Before running the project locally, make sure Apache Spark is properly configured in your environment.

---

## Run by source code

---

### Prerequisites

### Clone the Repository
```shell
git clone <this-repository-url>
cd data-refiner
```

### Download project runtime resources:
```shell
uv run dr_init && uv run dr_pack_resources
```

### Run Tests
Test all operators and pipelines:
```shell
uv run pytest
```

### Run pipeline
- Pipeline examples -> `tests/pipeline/pipeline_cfg_files`
- Code execution reference: [example](../../run_locally.py)

### Run user defined operator in pipeline
- Code execution reference: [example](../../run_locally_with_new_op.py)
---
## Run by `data-refiner` package
### Install package

```shell
uv pip install data-refiner-pyspark
```

### Download project runtime resources:
```shell
uv run dr_init && uv run dr_pack_resources
```

### Run pipeline
You can follow the `quick start` section on `README.md` or:
- Pipeline examples -> `tests/pipeline/pipeline_cfg_files`
- Code execution reference: [example](../../run_locally.py)

### Run user defined operator in pipeline
- Code execution reference: [example](../../run_locally_with_new_op.py)

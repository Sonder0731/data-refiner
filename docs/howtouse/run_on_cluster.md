# Running Data-refiner in Cluster Mode
This guide describes how to run Data Refiner on cluster, either directly from the source code or by installing the data-refiner Python package.

💡 If you don't have access to a production Spark cluster, [this project](https://github.com/Sonder0731/hadoop-hive-spark-docker) provides a Docker Compose-based local environment integrating HDFS, YARN, Spark, Hive, and
PostgreSQL Metastore for development and learning.
---

## Run by source code
### 1. Clone the Repository
```shell
git clone <this-repository-url>
cd data-refiner
```

### 2. Using conda to package python environment
- Create conda environment
  ```shell
  conda create -n data_refiner_env python=3.11
  ```
- Activate conda environment
  ```shell
  conda activate data_refiner_env
  ```
- Export dependencies
  ```shell
  uv run dr_export_dependency
  ```
- Install dependencies
  ```shell
  pip install -r requirements.txt
  pip install conda-pack
  ```
- Get env information
  ```shell
  conda info --env
  # base                     /home/sonder/miniconda3
  # data_refiner_env     *   /home/sonder/miniconda3/envs/data_refiner_env
  ```
- pack conda env, get `data_refiner_env.tar.gz`
  ```shell
  conda-pack \
  --prefix /home/sonder/miniconda3/envs/data_refiner_env \
  --output data_refiner_env.tar.gz \
  --format tar.gz \
  --compress-level 1 \
  --n-threads -1 \
  --force
  ```
### 2. Package `data-refiner` project as a whl package
- Under project root path
- Use uv to package project, get project `whl` file
  ```shell
  uv build -v
  ```
### 3. Package `data-refiner` runtime resources, get `data-refiner-runtime-resources.zip`
- Under project root path, run command
  ```shell
  uv run dr_init && uv run dr_pack_resources
  ```

### 4. Task submit command
**A example command:**
```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--num-executors 2 \
--executor-cores 1 \
--executor-memory 512M \
--jars graphframes-0.8.4-spark3.5-s_2.12.jar \
--archives data_refiner_env.tar.gz#PYTHON_ENV,data-refiner-runtime-resources.zip#data-refiner-runtime-resources \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PYTHON_ENV/bin/python \
--conf spark.executorEnv.PYSPARK_PYTHON=./PYTHON_ENV/bin/python \
--conf spark.executor.memoryOverhead=1G \
--files pipeline.yaml \
--py-files data_refiner_pyspark-0.1.2-py3-none-any.whl \
run_cluster_args.py --pipeline pipeline.yaml
```
**Explanation**

- `graphframes-0.8.4-spark3.5-s_2.12.jar`

  The GraphFrames JAR required by data-refiner.
  
  You can find it at: data_refiner/dependency/jar/graphframes-0.8.4-spark3.5-s_2.12.jar

- `data_refiner_env.tar.gz`

  The packaged Conda Python environment created in the previous environment packaging step.

- `data-refiner-runtime-resources.zip`

  The runtime resources required by data-refiner, packaged in the previous resources packaging step.

- `pipeline.yaml`

  Your pipeline configuration file.

- `data_refiner_pyspark-0.1.2-py3-none-any.whl`

  The data-refiner Python package built with uv build.

- `run_cluster_args.py`

  The PySpark entry script used to load the pipeline configuration and run the pipeline on the cluster.
  
  You can use the example script provided in:
  
  run_cluster_args.py

## Run by package installation

### 1. Clone the Repository
```shell
git clone <this-repository-url>
cd data-refiner
```

### 2. Using conda to package python environment
- Create conda environment
  ```shell
  conda create -n data_refiner_env python=3.11
  ```
- Activate conda environment
  ```shell
  conda activate data_refiner_env
  ```
- Install `data-refiner` package
  ```shell
  pip install data-refiner-pyspark
  pip install conda-pack
  ```
- Get env information
  ```shell
  conda info --env
  # base                     /home/sonder/miniconda3
  # data_refiner_env     *   /home/sonder/miniconda3/envs/data_refiner_env
  ```
- pack conda env, get `data_refiner_env.tar.gz`
  ```shell
  conda-pack \
  --prefix /home/sonder/miniconda3/envs/data_refiner_env \
  --output data_refiner_env.tar.gz \
  --format tar.gz \
  --compress-level 1 \
  --n-threads -1 \
  --force
  ```
  
### 3. Package `data-refiner` runtime resources, get `data-refiner-runtime-resources.zip`
```shell
dr_init && dr_pack_resources
```

### 4. Task submit command
**A example command:**
```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--num-executors 2 \
--executor-cores 1 \
--executor-memory 512M \
--jars graphframes-0.8.4-spark3.5-s_2.12.jar \
--archives data_refiner_env.tar.gz#PYTHON_ENV,data-refiner-runtime-resources.zip#data-refiner-runtime-resources \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PYTHON_ENV/bin/python \
--conf spark.executorEnv.PYSPARK_PYTHON=./PYTHON_ENV/bin/python \
--conf spark.executor.memoryOverhead=1G \
--files pipeline.yaml \
run_cluster_args.py --pipeline pipeline.yaml
```
**Explanation**

- `graphframes-0.8.4-spark3.5-s_2.12.jar`

  The GraphFrames JAR required by data-refiner.
  
  You can find it at: data_refiner/dependency/jar/graphframes-0.8.4-spark3.5-s_2.12.jar

- `data_refiner_env.tar.gz`

  The packaged Conda Python environment created in the previous environment packaging step.

- `data-refiner-runtime-resources.zip`

  The runtime resources required by data-refiner, packaged in the previous resources packaging step.

- `pipeline.yaml`

  Your pipeline configuration file.

- `run_cluster_args.py`

  The PySpark entry script used to load the pipeline configuration and run the pipeline on the cluster.
  
  You can use the example script provided in:
  
  run_cluster_args.py

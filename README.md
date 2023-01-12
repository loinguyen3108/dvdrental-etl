# ![DVD Rental](https://github.com/loinguyen3108/dvdrental-etl/blob/main/images/logo.gif?raw=true)

> This is example project for hadoop, spark, hive and superset

[![github release date](https://img.shields.io/github/release-date-pre/loinguyen3108/dvdrental-etl)](https://github.com/nhn/tui.editor/releases/latest) [![commit active](https://img.shields.io/github/commit-activity/w/loinguyen3108/dvdrental-etl)](https://github.com/loinguyen3108/dvdrental-etl/releases/tag/pyspark) [![license](https://img.shields.io/badge/license-Apache-blue)](https://github.com/nhn/tui.editor/blob/master/LICENSE) [![PRs welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/loinguyen3108/dvdrental-etl/issues) [![code with hearth by Loi Nguyen](https://img.shields.io/badge/DE-Loi%20Nguyen-orange)](https://github.com/loinguyen3108)

## 🚩 Table of Contents

- [](#)
  - [🚩 Table of Contents](#-table-of-contents)
  - [🎨 Stack](#-stack)
    - [⚙️ Setup](#️-setup)
  - [✍️ Example](#️-example)
- [](#-1)
- [](#-2)
  - [📜 License](#-license)

## 🎨 Stack

    Project run in local based on `docker-compose.yml` in [bigdata-stack](https://github.com/loinguyen3108/bigdata-stack)

### ⚙️ Setup

**1. Run bigdata-stack**
```
git clone git@github.com:loinguyen3108/bigdata-stack.git

cd bigdata-stack

docker compose up -d
```

**2. Spark Standalone**
Setup at [spark](https://spark.apache.org/docs/latest/spark-standalone.html)

**3. Dataset**
Data is downloaded at [PostgreSQL Sample Database](https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/)

**4. Environment**
```
export JDBC_URL=...
export JDBC_USER=...
export JDBC_PASSWORD=...
```

**5. Build dependencies**
```
./build_dependencies.sh
```

**6. Insert local packages**
```
./update_local_packages.sh
```

## ✍️ Example
- Data Lake
# ![Data Lake](https://github.com/loinguyen3108/dvdrental-etl/blob/main/images/datalake.png?raw=true)
- Hive
# ![Hive](https://github.com/loinguyen3108/dvdrental-etl/blob/main/images/hive.png?raw=true)
## 📜 License

This software is licensed under the [Apache](https://github.com/loinguyen3108/dvdrental-etl/blob/master/LICENSE) © [Loi Nguyen](https://github.com/loinguyen3108).

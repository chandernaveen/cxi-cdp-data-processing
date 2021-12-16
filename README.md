# Introduction 

Repository that contains Apache Spark jobs for data processing inside CXI Cloud Data Platform.

### Prerequisites
Databricks runtime 9.1 - in which these Apache Spark jobs are executed -
has following [system environment](https://docs.microsoft.com/en-gb/azure/databricks/release-notes/runtime/9.1#system-environment),
so locally we need the following components installed: 

- [Python 3.8.8](https://www.python.org/downloads/release/python-388/) - required for `databricks-connect` Python package
- [Scala 2.12.10](https://www.scala-lang.org/download/2.12.10.html) - language that Apache Spark jobs are written in
- [sbt 1.4.3](https://www.scala-sbt.org/download.html) - Scala build tool
- [Java 1.8](https://adoptopenjdk.net/?variant=openjdk8&jvmVariant=hotspot) (dependency for Scala & sbt)
- Other dependencies will be installed when you execute the following commands _one time_ in project root directory:
  ```bash
    python -m venv .venv
    sbt setupDatabricksConnect
    ```
- Create your own dedicated Databricks `Single Node cluster` in your Azure subscription 
  and execute `databricks-connect configure` command locally and [configure its properties](https://docs.databricks.com/dev-tools/databricks-connect.html#step-2-configure-connection-properties) 
- Create in project root folder file with name `env.properties` and following word `local` as content, 
  this is required in order to configure `SparkSession` properly and distinguish between local development and execution in dev/staging/prod environments.

Afterwords follow instructions to finish your IDE setup:
- https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#set-up-your-ide-or-notebook-server

### What is Databricks-Connect ?
```
Databricks Connect is a client library for Databricks Runtime 
that we use for local development (and in CI/CD) and that has all lib dependencies 
in-place to develop and test against specific Databricks runtime version.
```
Links: 
- https://docs.microsoft.com/en-gb/azure/databricks/dev-tools/databricks-connect
- https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#limitations
- https://docs.microsoft.com/en-us/azure/databricks/jobs#--jar-job-tips
- https://docs.microsoft.com/en-us/azure/databricks/data/filestore

### Known issues
- To access the DBUtils module in a way that works both locally and in Azure Databricks clusters, use the following import:
  
  `val dbutils = com.databricks.service.DBUtils` instead of `com.databricks.dbutils_v1.dbutils`
  
  https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#scala-2

- In order to obtain `SparkSession` use `com.cxi.cdp.data_processing.support.SparkSessionFactory.getSparkSession` function,
  as it takes care of determining the environment it is created in (i.e. spark job is executed from your laptop, or it's already running on the Databricks cluster).

- Running locally MongoDB Spark jobs using Databricks Connect.
`Exception in thread "main" org.apache.spark.SparkException: This DataFrame operation is not currently supported by Databricks Connect.` - when trying to write DataFrame sourced from Mongo collection. Workaround: run job from ADF/Databricks instead.

### Contribute

1. Create new branch with your ticket number e.g. `DP-123`
2. Apply the changes
3. Commit
4. Push to remote repo and create PR
5. After PR is reviewed and approved, merge to main branch
6. Deploy changes to all environments.

### How to test
```
sbt test
```
### How to build
```
sbt assembly
```

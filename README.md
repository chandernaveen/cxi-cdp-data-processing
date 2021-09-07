# Introduction 

Repository that contains Apache Spark jobs for data processing inside CXI Cloud Data Platform.

## Prerequisites
Databricks runtime 8.1 - in which these Apache Spark jobs are executed -
has following [system environment](https://docs.microsoft.com/en-gb/azure/databricks/release-notes/runtime/8.1#system-environment),
so locally we need the following components installed: 

- Python 3.8.8 - required for `databricks-connect` Python package
- Scala 2.12.10 - language that Apache Spark jobs are written in
- sbt 1.4.3 - Scala build tool
- Java 1.8 (dependency for Scala & sbt)
- Other dependencies will be installed when you execute the following commands _one time_ in project root directory:
  ```bash
    python -m venv .venv
    sbt setupDatabricksConnect
    ```
- Create your own dedicated Databricks `Single Node cluster` in your Azure subscription 
  and execute `databricks-connect configure` command locally and [configure its properties](https://docs.databricks.com/dev-tools/databricks-connect.html#step-2-configure-connection-properties) 
  
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

## How to test
```
sbt test
```
## How to build
```
sbt assembly
```

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

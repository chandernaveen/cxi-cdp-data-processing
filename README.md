# Introduction 

Repository that contains Apache Spark jobs for data processing inside CXI Cloud Data Platform.

## Prerequisites
Databricks runtime 8.1 - in which these Apache Spark jobs jobs are executed -
has following [system environment](https://docs.microsoft.com/en-gb/azure/databricks/release-notes/runtime/8.1#system-environment),
so locally we need the following components installed: 

- Python 3.8.8 - required for (databricks-connect) [#Databricks-Connect] python package
- sbt 1.4.3 - build tool
- Scala 2.12.10 - language that Apache Spark jobs are written in

Other dependencies & setup will be installed automatically when you execute the commands in project root directory:
```bash
python -m venv .venv
sbt setupDatabricksConnect && sbt assembly
```

### Databricks-Connect
```
Databricks Connect is a client library for Databricks Runtime 
that we use local development (and in CI/CD) and that has all lib dependencies 
in-place to develop and test against specific Databricks runtime version.
```
Links: 
- https://docs.microsoft.com/en-gb/azure/databricks/dev-tools/databricks-connect
- https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#limitations
- https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#set-up-your-ide-or-notebook-server

## Build and Test
How to test:
```
sbt setupDatabricksConnect && sbt test
```
How to build:
```
sbt setupDatabricksConnect && sbt assembly
```

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

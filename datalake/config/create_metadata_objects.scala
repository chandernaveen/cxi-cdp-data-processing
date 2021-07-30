// Databricks notebook source
/*
dbutils.widgets.removeAll()
dbutils.widgets.text("appAuditTableName","application_audit_logs","")
*/

// COMMAND ----------

// Define variables

val workspace_host    = dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String,String]]("browserHostName")
val workspace_id      = dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String,String]]("orgId")
val appAuditTableName = dbutils.widgets.get("appAuditTableName")
val appAuditTableLocation    =   s"/mnt/logs/databricks/$workspace_host/?o=$workspace_id/application_audit_logs"
val appAuditTableLocationStg =   s"/mnt/logs/databricks/$workspace_host/?o=$workspace_id/testing_application_audit_logs"

var recordName : String = null
var position   : Integer = null
var recordId   : Integer = null

var errorRows = List("Errors")

var rows = spark.createDataFrame(Seq(
        (null,null,null,null,null,null)
        )).toDF("recordId", "recordName", "position","fieldName", "dataType", "nullable")

// COMMAND ----------

// Create Metadata Database
try 
{
  spark.sql(s"CREATE DATABASE logs") 
   println(s"Created Database logs")
}
catch 
{
  case e: org.apache.spark.sql.AnalysisException =>
   println(s"Database '$databaseName' Already Exists")
}
try 
{
  spark.sql(s"CREATE DATABASE testing") 
   println(s"Created Database testing")
}
catch 
{
  case e: org.apache.spark.sql.AnalysisException =>
   println(s"Database '$databaseName' Already Exists")
}

// COMMAND ----------

// Create Application Audit Log Table
try
{
spark.sql(s"""
CREATE TABLE logs.$appAuditTableName (
 processName string ,
 entity string ,
 runID int ,
 dpYear int ,
 dpMonth int ,
 dpDay int ,
 dpHour int ,
 dpPartition string ,
 subEntity string ,
 processStartTime timestamp ,
 processEndTime timestamp ,
 writeStatus int ,
 errorMessage string,
 readRowCount int
 )
 USING DELTA
 PARTITIONED BY (processName,entity)
 LOCATION "$appAuditTableLocation"
 """)
 }
catch 
{
  case e: org.apache.spark.sql.AnalysisException =>
   println("Table 'staging.$appAuditTableName' Already Exists")
}
try
{
  spark.sql(s"""
  CREATE TABLE testing.$appAuditTableName (
   processName string ,
   entity string ,
   runID int ,
   dpYear int ,
   dpMonth int ,
   dpDay int ,
   dpHour int ,
   dpPartition string ,
   subEntity string ,
   processStartTime timestamp ,
   processEndTime timestamp ,
   writeStatus int ,
   errorMessage string,
   readRowCount int
   )
   USING DELTA
   PARTITIONED BY (processName,entity)
   LOCATION "$appAuditTableLocationStg"
   """) 
}
catch 
{
  case e: org.apache.spark.sql.AnalysisException =>
   println("Database 'staging.$appAuditTableName' Already Exists")
}

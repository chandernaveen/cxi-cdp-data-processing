package com.cxi.cdp.data_processing
package support.config

import org.apache.spark.sql.SparkSession

object CreateMetadataObjects {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            throw new RuntimeException("Expected 3 args, but got: " + args.mkString("Array(", ", ", ")"))
        }
        val workspace_host = args(0) //dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String, String]]("browserHostName")
        val workspace_id = args(1) //dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String, String]]("orgId")
        val appAuditTableName = args(2) //dbutils.widgets.get("appAuditTableName")
        val appAuditTableLocation = s"/mnt/logs/databricks/$workspace_host/?o=$workspace_id/application_audit_logs"
        val appAuditTableLocationStg = s"/mnt/logs/databricks/$workspace_host/?o=$workspace_id/testing_application_audit_logs"
        val spark = SparkSession.builder().getOrCreate()
        run(spark, appAuditTableLocation, appAuditTableLocationStg, appAuditTableName)
    }

    def run(spark: SparkSession, appAuditTableLocation: String, appAuditTableLocationStg: String, appAuditTableName: String): Unit = {
        var recordName: String = null
        var position: Integer = null
        var recordId: Integer = null

        var errorRows = List("Errors")

        var rows = spark.createDataFrame(Seq(
            (null, null, null, null, null, null)
        )).toDF("recordId", "recordName", "position", "fieldName", "dataType", "nullable")

        // COMMAND ----------
        val databaseName = "logs"

        // Create Metadata Database
        try {
            spark.sql(s"CREATE DATABASE $databaseName")
            println(s"Created Database $databaseName")
        }
        catch {
            case e: org.apache.spark.sql.AnalysisException =>
                println(s"Database '$databaseName' Already Exists")
        }
        try {
            spark.sql(s"CREATE DATABASE testing")
            println(s"Created Database testing")
        }
        catch {
            case e: org.apache.spark.sql.AnalysisException =>
                println(s"Database '$databaseName' Already Exists")
        }

        // COMMAND ----------

        // Create Application Audit Log Table
        try {
            spark.sql(
                s"""
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
        catch {
            case e: org.apache.spark.sql.AnalysisException =>
                println("Table 'staging.$appAuditTableName' Already Exists")
        }
        try {
            spark.sql(
                s"""
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
        catch {
            case e: org.apache.spark.sql.AnalysisException =>
                println(s"Database 'staging.$appAuditTableName' Already Exists")
        }

    }
}

// Databricks notebook source
package com.cxi.cdp.data_processing
package support.packages.utils

import io.delta.tables._
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._

/**
  *
  * Object to simplify the use of Delta Functionality for CXi Data Platform
  *
  **/

object DeltaTableFunctions {

  lazy val sparkSession = SparkSession.builder.getOrCreate()

  /**
    *
    * @param tableName
    * table name is schema plus table name for example "schema.tablename"
    */
  def tableExists(tableName: String): Boolean = {
    if (sparkSession.catalog.tableExists(tableName)) {
      true
    }
    else {
      throw new IllegalArgumentException(s"Table ${tableName} does not exists")
    }
  }


  /**
    *
    * @param tableName
    * table name is schema plus table name for example "schema.tablename"
    * @return
    */
  def getTableDescribe(tableName: String): Array[org.apache.spark.sql.Row] = {
    sparkSession.sql(s"DESCRIBE FORMATTED ${tableName}").collect()
  }

  def getTableDescribe(path: java.nio.file.Path): Array[org.apache.spark.sql.Row] = {
    sparkSession.sql(s"DESCRIBE FORMATTED delta.`${path.toString}`").collect()
  }



  /**
    * getLocation - gettig location
    * table name is schema plus table name for example "schema.tablename"
    */
  def getLocation(tableName: String): String = {
    tableExists(tableName)
    val describedValues = getTableDescribe(tableName)
    describedValues.find(x => (x.getString(0) == "Location")).get.getString(1).substring(5) // dbfs:/mnt/
    // !!! see row table in table
  }

  /**
    *
    * @param schema
    * @param tableName
    * @return
    */
  def getLocation(schema: String, tableName: String): String = {
    getLocation( if (schema.trim.isEmpty) tableName else schema + "." + tableName)
  }

  def getDetailedTableInformation(path: java.nio.file.Path) = {
    val describedValues = getTableDescribe(path)
    val indexOf = describedValues.indexWhere(x => (x.getString(0).indexOf("# Detailed Table Information") != -1))
    val length = describedValues.size - indexOf - 1
    val arr = new Array[Row](length)
    Array.copy(describedValues, indexOf + 1,  arr, 0, length)
    arr
  }

  def getTableName(path: java.nio.file.Path) : (String, String) = {
    val detailInfo = getDetailedTableInformation(path)
    val indexOf = detailInfo.indexWhere(x => (x.getString(0).indexOf("Name") != -1))

    ("","")
  }

  /**
    *
    * @param desc
    * @return
    */
  private def getFieldlist(desc: Array[org.apache.spark.sql.Row]): Array[org.apache.spark.sql.Row] = {
    val indexOf = desc.indexWhere(x => (x.getString(0).indexOf("#") != -1 || x.getString(0).trim.isEmpty))
    desc.take(indexOf)
  }

  /**
    *
    * @param tableName
    * table name is schema plus table name for example "schema.tablename"
    * @return
    */
  def getFieldList(tableName: String): Array[org.apache.spark.sql.Row] = {
    tableExists(tableName)
    getFieldlist(getTableDescribe(tableName))
  }


  def getFieldList(path: java.nio.file.Path): Array[org.apache.spark.sql.Row] = {
    getFieldlist(getTableDescribe(path))
  }

  def getPartitions(path: java.nio.file.Path): Array[org.apache.spark.sql.Row] = {
    sparkSession.sql(s"SHOW PARTITIONS delta.`${path.toString}`").collect()
  }

  def getPartitionsDataFrame(path: java.nio.file.Path): DataFrame = {
    val partitions = getPartitions(path)
    if (partitions.length != 0) {
      val schema = partitions(0).schema
      sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(partitions), schema)
    }
    else
      null
  }


  def getPartitionFieldList(tableName: String): Array[org.apache.spark.sql.Row] = {
    tableExists(tableName)
    sparkSession.sql(s"DESCRIBE DETAIL ${tableName}").select(explode(col("partitionColumns"))).collect()
  }


  def getPartitionFieldList(path: java.nio.file.Path): Array[org.apache.spark.sql.Row] = {
    sparkSession.sql(s"DESCRIBE DETAIL delta.`${path.toString}`").select(explode(col("partitionColumns"))).collect()
  }


  def VacuumTable(path: java.nio.file.Path, hours: Double = 168) = {
    try {
      sparkSession.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      val deltaTable = DeltaTable.forPath(path.toString)
      deltaTable.vacuum(hours) // Default described above
    }
    catch {
      case e: Throwable =>
        throw new RuntimeException(e)
    }
    finally {
      sparkSession.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    }
  }
}

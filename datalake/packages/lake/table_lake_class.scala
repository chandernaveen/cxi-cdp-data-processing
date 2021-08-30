// Databricks notebook source
// MAGIC %run "../utils/delta_utils"

// COMMAND ----------

package com.cxi.lake

import com.cxi.utils.DeltaTableFunctions

abstract class TableLake(val path: String) extends Serializable {

  import java.util.UUID.randomUUID
  import io.delta.tables._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  import scala.collection.mutable
  import org.apache.spark.sql._
  import org.apache.spark.sql.SparkSession
  
  /**
    * Additonal constructor for the class
    *
    * @param sparkSession
    * @param schema    - schema of table
    * @param tableName - name of table
    */
  def this(schema: String, tableName: String) {
    this(DeltaTableFunctions.getLocation(schema, tableName))
  }
  
  @transient
  lazy val table = DeltaTable.forPath(path.toString).as("tbl")
  lazy val fieldList = DeltaTableFunctions.getFieldList(java.nio.file.Paths.get(path) )
  lazy val sparkSession = SparkSession.builder.getOrCreate()
  import sparkSession.implicits._

  val generateUUID = udf(() => randomUUID().toString)
  val requiredFields: Seq[String]
  val trgAlias = "ups"
  var mapping: mutable.Map[String, String] = mutable.Map()
  var applyFunction: mutable.Map[String, String] = mutable.Map()


  /**
    *
    * @param df
    * @return
    */
  def checkRequiredFields(df: DataFrame): Boolean = {
    df.columns.intersect(requiredFields).length == requiredFields.length
  }

  protected def merge(df: DataFrame): Unit

  def listOfRequiredFields(): String = {
    requiredFields.mkString(",")
  }

  /**
    * Merging df into request tracking table
    *
    * @param df - data frame for merging
    */
  def upsert(df: DataFrame): Unit = {
    if (checkRequiredFields(df)) {
      merge(df)
    }
    else
      throw new IllegalArgumentException(s"There aren't required fields (${listOfRequiredFields()}) in input dataframe.")
  }

  def delete(idsForDeletions: DataFrame
            ,conditionOfJoin: String): Unit = {
  }  
  
   def toDf : DataFrame = {
    table.toDF
  }

}

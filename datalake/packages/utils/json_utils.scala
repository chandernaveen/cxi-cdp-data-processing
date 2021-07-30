// Databricks notebook source
package com.cxi.utils

object JsonUtils extends Serializable {

  import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
  import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.Dataset
  import org.apache.spark.sql.types._  
  import scala.util.{Try, Success, Failure}
  import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
  import org.apache.spark.sql._

  lazy val sparkSession = SparkSession.builder.getOrCreate()
  
  
  lazy val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k, v) => k.name -> v })
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json: String)(implicit m: Manifest[V]) = fromJson[Map[String, V]](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }
  
  def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
    val reader = sparkSession.read
    Option(schema).foreach(reader.schema)
    reader.json(json)
  }
  
  def deserializeSchema(json: String): StructType = {
    Try(DataType.fromJson(json).asInstanceOf[StructType]) match {
        case Success(t) => t
        case Failure(e) => throw new RuntimeException(s"Failed parsing StructType: $json. ${e.toString}")
    }
  }
  
  def readJSONSchema(path: String): StructType = {
    val df_json = sparkSession.sparkContext.wholeTextFiles(path).take(1)(0)._2 
    val st = deserializeSchema(df_json)
    st
  }
  
  def readJSONSchemaSTR(path: String): String = {
    val df_json = sparkSession.sparkContext.wholeTextFiles(path).take(1)(0)._2 
    df_json
  }
  
  def prettyPrinter(value: Object) : String = {
   val writer = mapper.writerWithDefaultPrettyPrinter
   writer.writeValueAsString(value)
  }
  
}

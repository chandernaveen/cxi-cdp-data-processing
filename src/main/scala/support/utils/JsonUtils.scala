package com.cxi.cdp.data_processing
package support.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql._

object JsonUtils extends Serializable {
    lazy val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    def toMap[V](json: String)(implicit m: Manifest[V]): Map[String, V] = fromJson[Map[String, V]](json)

    def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
        mapper.readValue[T](json)
    }

    def readFileContentAsString(path: String)(implicit sparkSession: SparkSession): String = {
        sparkSession.sparkContext.wholeTextFiles(path).take(1)(0)._2
    }

    def prettyPrinter(value: Object): String = {
        val writer = mapper.writerWithDefaultPrettyPrinter
        writer.writeValueAsString(value)
    }

}

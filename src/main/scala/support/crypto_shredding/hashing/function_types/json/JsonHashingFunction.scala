package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import support.crypto_shredding.hashing.function_types.IHashFunction
import support.exceptions.CryptoShreddingException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class JsonHashingFunction(val hashFunctionConfig: Map[String, Any], val salt: String) extends IHashFunction {

    private val piiConfig: PiiColumnsConfig =
        PiiColumnsConfig.parse(hashFunctionConfig("pii_columns").asInstanceOf[Seq[Map[String, Any]]])

    override def hash(originalDf: DataFrame): (DataFrame, DataFrame) = {
        val originalSchemaPlusColumnForHashedData: StructType = originalDf.schema
            .add("hashed_data", DataTypes.createArrayType(
                DataTypes.createStructType(Array(
                    StructField("original_value", StringType, nullable = true),
                    StructField("hashed_value", StringType, nullable = true))
                ),
                true),
                nullable = true)

        val hashedOriginalDf = originalDf
            // hash and record pii data, serializes row data to one json string column
            .mapPartitions(hashPartition)(RowEncoder.apply(StructType(Array(StructField("json_row", StringType, nullable = false)))))
            // deserialize json back according to existing schema + new hashed_data field
            .withColumn("json_row", from_json(col("json_row"), originalSchemaPlusColumnForHashedData))
            .select("json_row.*")

        val extractedPersonalInformationLookupDf = hashedOriginalDf
            .withColumn("hashed_data", explode_outer(col("hashed_data")))
            .select("hashed_data.*")
            .filter(col("original_value").isNotNull and col("hashed_value").isNotNull)

        (hashedOriginalDf.drop("hashed_data"), extractedPersonalInformationLookupDf)
    }

    def hashPartition(iter: Iterator[Row]): Iterator[Row] = {
        val mapper = new ObjectMapper() with ScalaObjectMapper // create once per executor
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        val jsonNodeHasher = new JsonNodeHasher(piiConfig, salt, mapper)

        iter.map(hashRow(jsonNodeHasher, mapper))
    }

    private def hashRow(jsonNodeHasher: JsonNodeHasher, mapper: ObjectMapper with ScalaObjectMapper)(row: Row) = {
        try {
            val res = jsonNodeHasher.apply(mapper.readTree(row.json))
            Row(res.toString)
        } catch {
            case e: Exception =>
                throw new CryptoShreddingException(s"Failed to apply json hashing to the following row: ${row.json}", e)
        }
    }

    override def getType: String = "json-crypto-hash"

}

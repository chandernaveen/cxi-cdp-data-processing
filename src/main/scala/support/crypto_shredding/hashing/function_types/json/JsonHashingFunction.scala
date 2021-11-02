package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import support.crypto_shredding.hashing.Hash
import support.crypto_shredding.hashing.function_types.IHashFunction
import support.exceptions.CryptoShreddingException

import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class JsonHashingFunction(val hashFunctionConfig: Map[String, Any], val salt: String) extends IHashFunction {

    private val piiFields: List[Tuple2[String, String]] = hashFunctionConfig("pii_columns").asInstanceOf[List[Any]].map(columnConfig => {
        val columnConfigMap = columnConfig.asInstanceOf[Map[String, Any]]
        Tuple2(
            columnConfigMap("outerColName").asInstanceOf[String],
            columnConfigMap("innerColName").asInstanceOf[String]
        )
    })

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
            .mapPartitions(hashPartition)(RowEncoder.apply(StructType(Array(StructField("json_row", StringType, nullable = false))))) // hash and record pii data, serializes row data to one json string column
            .withColumn("json_row", from_json(col("json_row"), originalSchemaPlusColumnForHashedData)) // deserialize json back according to existing schema + new hashed_data field
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
        iter.map(row => hashRow(mapper, row))
    }

    private def hashRow(mapper: ObjectMapper with ScalaObjectMapper, row: Row) = {
        try {
            val res: ObjectNode = mapper.readTree(row.json).deepCopy()
            val hashedData = res.putArray("hashed_data")

            for (piiField <- piiFields) {
                val (outerColName, innerColName) = piiField
                if (res.has(outerColName)) {
                    val node = res.get(outerColName)

                    val innerImmutableContent: JsonNode = if (node.isTextual) mapper.readTree(node.asText()) else node // if we already processed outerCol before we are working not with string but reusing actual node
                    if (innerImmutableContent.has(innerColName)) {
                        val innerMutableContent: ObjectNode = innerImmutableContent.deepCopy() // do copy only if we have something to update

                        val originalValue: JsonNode = innerMutableContent.get(innerColName)
                        val hashedValue: String = Hash.sha256Hash(originalValue.asText(), salt)

                        // update original row replacing pii data with hash
                        innerMutableContent.set(innerColName, TextNode.valueOf(hashedValue))
                        res.set(outerColName, innerMutableContent)

                        // extract pii info to a separate top level column
                        val entry = hashedData.addObject()
                        entry.put("original_value", originalValue.asText())
                        entry.put("hashed_value", hashedValue)
                    }
                }
            }
            Row(res.toString)
        } catch {
            case e: Exception =>
                throw new CryptoShreddingException(s"Failed to apply json hashing to the following row: ${row.json}", e)
        }
    }

    override def getType: String = "json-crypto-hash"

}

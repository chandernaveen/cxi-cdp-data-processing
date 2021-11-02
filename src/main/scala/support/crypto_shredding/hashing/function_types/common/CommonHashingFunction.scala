package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

import support.crypto_shredding.hashing.Hash
import support.crypto_shredding.hashing.function_types.IHashFunction

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}

class CommonHashingFunction(val hashFunctionConfig: Map[String, Any], val salt: String) extends IHashFunction {
    private val dataColName: String = hashFunctionConfig("dataColName").asInstanceOf[String]

    override def hash(originalDf: DataFrame): (DataFrame, DataFrame) = {
        val hashUdf = createHashUdf()
        val hashedOriginalDf = originalDf.withColumn("hashed_value", hashUdf(col(dataColName)))

        val extractedPersonalInformationLookupDf = hashedOriginalDf
            .select(col(dataColName).as("original_value"), col("hashed_value"))

        (
            hashedOriginalDf
            .withColumn(dataColName, col("hashed_value"))
            .select(originalDf.schema.fieldNames.map(col):_*),

            extractedPersonalInformationLookupDf
        )
    }

    private def createHashUdf(): UserDefinedFunction = udf((value: String) => {
        val res: CommonOutputValue = valueHash(value)
        res.hash
    })

    private def valueHash(originalPii: String): CommonOutputValue = {
        try {
            val pii = originalPii.trim
            if (validate(pii)) {
                CommonOutputValue(isSucceeded = true, pii, Hash.sha256Hash(pii, salt))
            } else {
                CommonOutputValue(isSucceeded = false, pii, hash = "The validation failed")
            }
        } catch {
            case e: Throwable =>
                CommonOutputValue(isSucceeded = false, originalPii, hash = s"Exception ${e.getClass.getName} happened. ${e.toString}")
        }
    }

    private def validate(pii: String): Boolean = pii.nonEmpty

    override def getType: String = "common-crypto-hash"

}

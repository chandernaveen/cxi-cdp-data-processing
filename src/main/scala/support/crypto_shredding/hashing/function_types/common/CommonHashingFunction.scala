package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

import support.crypto_shredding.hashing.function_types.{CryptoHashingResult, IHashFunction}
import support.crypto_shredding.hashing.Hash

import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

class CommonHashingFunction(piiConfig: PiiColumnsConfig, salt: String) extends IHashFunction {

    def this(hashFunctionConfig: Map[String, Any], salt: String) {
        this(PiiColumnsConfig.parse(hashFunctionConfig("pii_columns").asInstanceOf[Seq[Map[String, Any]]]), salt)
    }

    private val normalizedValueCol = "normalized_value"

    override def hash(originalDf: DataFrame): (DataFrame, Dataset[CryptoHashingResult]) = {
        val session = originalDf.sparkSession
        val extractedPersonalInformationLookupDf =
            session.emptyDataset[CryptoHashingResult](Encoders.product[CryptoHashingResult])

        val hashUdf = createHashUdf()

        val (hashedOriginalRes, extractedPersonalInformationLookupDfRes) =
            piiConfig.columns.foldLeft((originalDf, extractedPersonalInformationLookupDf)) {
                case ((accHashed, accExtracted), piiField) =>
                    val (dataColName, transformFunction, identityTypeOpt) = piiField
                    val normalizeFunction = udf(transformFunction)
                    val dfWithHashedColumn = accHashed
                        .withColumn(s"$normalizedValueCol-$dataColName", normalizeFunction(col(dataColName)))
                        .withColumn(
                            s"${CryptoHashingResult.HashedValueColName}-$dataColName",
                            hashUdf(col(s"$normalizedValueCol-$dataColName"))
                        )

                    val colHashedRes = dfWithHashedColumn
                        .withColumn(dataColName, col(s"${CryptoHashingResult.HashedValueColName}-$dataColName"))
                        .select(originalDf.schema.fieldNames.map(col): _*)

                    val colExtractedRes = accExtracted.unionByName(
                        dfWithHashedColumn
                            .select(
                                col(s"$normalizedValueCol-$dataColName").as(CryptoHashingResult.OriginalValueColName),
                                col(s"${CryptoHashingResult.HashedValueColName}-$dataColName")
                                    .as(CryptoHashingResult.HashedValueColName),
                                lit(identityTypeOpt.map(_.code).orNull).as(CryptoHashingResult.IdentityTypeValueColName)
                            )
                            .as(Encoders.product[CryptoHashingResult])
                            .filter(chr => chr.original_value != null && chr.hashed_value != null)
                    )

                    (colHashedRes, colExtractedRes)
            }

        (hashedOriginalRes, extractedPersonalInformationLookupDfRes)
    }

    private def createHashUdf(): UserDefinedFunction = udf((value: String) => {
        val res: CommonOutputValue = valueHash(value)
        Some(res).filter(_.isSucceeded).map(_.hash).orNull
    })

    private def valueHash(originalPii: String): CommonOutputValue = {
        try {
            if (validate(originalPii)) {
                val pii = originalPii.trim
                CommonOutputValue(isSucceeded = true, pii, Hash.sha256Hash(pii, salt))
            } else {
                CommonOutputValue(isSucceeded = false, originalPii, hash = "The validation failed")
            }
        } catch {
            case e: Throwable =>
                CommonOutputValue(
                    isSucceeded = false,
                    originalPii,
                    hash = s"Exception ${e.getClass.getName} happened. ${e.toString}"
                )
        }
    }

    private def validate(pii: String): Boolean = pii != null && pii.trim.nonEmpty

}

package com.cxi.cdp.data_processing
package support.crypto_shredding

import support.WorkspaceConfigReader
import support.crypto_shredding.hashing.function_types.IHashFunction
import support.crypto_shredding.hashing.write.LookupTable
import support.crypto_shredding.hashing.{HashFunctionFactory, Salt}

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class CryptoShredding(val spark: SparkSession,
                      val country: String,
                      val cxiPartnerId: String,
                      val lookupDestDbName: String,
                      val lookupDestTableName: String,
                      val workspaceConfigPath: String) {

    def applyHashCryptoShredding(hashFunctionType: String,
                                 hashFunctionConfig: Map[String, Any],
                                 df: DataFrame): DataFrame = {

        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)

        val salt = Salt.getSalt(workspaceConfig)

        val function: IHashFunction = HashFunctionFactory.getFunction(hashFunctionType, hashFunctionConfig, salt)

        val (hashedOriginalDf, extractedPersonalInformationDf) = function.hash(df)

        val lookupDf = extractedPersonalInformationDf
            .withColumn("process_name", lit(function.getType))
            .withColumn("country", lit(country))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))

        val privacyFunctions = new PrivacyFunctions(spark, workspaceConfig)

        try {
            privacyFunctions.authorize()
            new LookupTable(spark, lookupDestDbName, lookupDestTableName).upsert(lookupDf)
            hashedOriginalDf
        } finally {
            privacyFunctions.unauthorize()
        }
    }

}

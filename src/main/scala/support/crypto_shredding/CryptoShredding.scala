package com.cxi.cdp.data_processing
package support.crypto_shredding

import support.WorkspaceConfigReader
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.hashing.function_types.IHashFunction
import support.crypto_shredding.hashing.write.LookupTable
import support.crypto_shredding.hashing.{HashFunctionFactory, Salt}

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class CryptoShredding(val spark: SparkSession, config: CryptoShreddingConfig) {

    def applyHashCryptoShredding(hashFunctionType: String,
                                 hashFunctionConfig: Map[String, Any],
                                 df: DataFrame): DataFrame = {

        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, config.workspaceConfigPath)

        val salt = Salt.getSalt(workspaceConfig)

        val function: IHashFunction = HashFunctionFactory.getFunction(hashFunctionType, hashFunctionConfig, salt)

        val (hashedOriginalDf, extractedPersonalInformationDf) = function.hash(df)

        val lookupDf = extractedPersonalInformationDf

        val lookupDfOld = extractedPersonalInformationDf
            .withColumn("process_name", lit(function.getType))
            .withColumn("country", lit(config.country))

        val privacyFunctions = new PrivacyFunctions(spark, workspaceConfig)

        try {
            privacyFunctions.authorize()
            new LookupTable(spark, config.lookupDestDbName, config.lookupDestTableName).upsert(lookupDf, config.cxiSource, config.dateRaw, config.runId)
            //    TODO: remove below code after https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
            new LookupTable(spark, config.lookupDestDbName, config.lookupDestTableName).upsertOld(lookupDfOld, config.cxiSource)
            hashedOriginalDf
        } finally {
            privacyFunctions.unauthorize()
        }
    }

}

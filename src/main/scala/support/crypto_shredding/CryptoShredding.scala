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

        val privacyFunctions = new PrivacyFunctions(spark, workspaceConfig)

        try {
            privacyFunctions.authorize()
            val lookupTable = new LookupTable(spark, config.lookupDestDbName, config.lookupDestTableName)
            lookupTable.upsert(extractedPersonalInformationDf, config.cxiSource, config.dateRaw, config.runId)
            hashedOriginalDf
        } finally {
            privacyFunctions.unauthorize()
        }
    }

}

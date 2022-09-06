package com.cxi.cdp.data_processing
package support.crypto_shredding

import support.{WorkspaceConfig, WorkspaceConfigReader}
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.hashing.{HashFunctionFactory, Salt}
import support.crypto_shredding.hashing.function_types.{CryptoHashingResult, IHashFunction}
import support.crypto_shredding.hashing.write.LookupTable
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class CryptoShredding(val spark: SparkSession, config: CryptoShreddingConfig) {

    def applyHashCryptoShredding(
        hashFunctionType: String,
        hashFunctionConfig: Map[String, Any],
        df: DataFrame
    ): DataFrame = {

        val workspaceConfig = getWorkspaceConfig(spark, config)

        val salt = getSalt(workspaceConfig)

        val function: IHashFunction = HashFunctionFactory.getFunction(hashFunctionType, hashFunctionConfig, salt)

        val (hashedOriginalDf, extractedPersonalInformationDf) = function.hash(df)

        upsertToPrivacyTable(extractedPersonalInformationDf, spark, config, workspaceConfig)
        hashedOriginalDf
    }

    def getWorkspaceConfig(spark: SparkSession, config: CryptoShreddingConfig): WorkspaceConfig = {
        WorkspaceConfigReader.readWorkspaceConfig(spark, config.workspaceConfigPath)
    }

    def getSalt(workspaceConfig: WorkspaceConfig): String = {
        Salt.getSalt(workspaceConfig)
    }

    def upsertToPrivacyTable(
        extractedPersonalInformationDf: Dataset[CryptoHashingResult],
        spark: SparkSession,
        config: CryptoShreddingConfig,
        workspaceConfig: WorkspaceConfig
    ): Unit = {
        inAuthorizedContext(spark, workspaceConfig) {
            val lookupTable = new LookupTable(spark, config.lookupDestDbName, config.lookupDestTableName)
            lookupTable.upsert(extractedPersonalInformationDf, config.cxiSource, config.dateRaw, config.runId)
        }
    }

}

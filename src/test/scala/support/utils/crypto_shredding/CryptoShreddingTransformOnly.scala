package com.cxi.cdp.data_processing
package support.utils.crypto_shredding

import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.hashing.function_types.CryptoHashingResult
import support.crypto_shredding.CryptoShredding
import support.WorkspaceConfig

import org.apache.spark.sql.{Dataset, SparkSession}

/** For using in tests instead of [[com.cxi.cdp.data_processing.support.crypto_shredding.CryptoShredding]].
  * Does crypto-shredding but does not write to the Privacy Lookup table.
  */
class CryptoShreddingTransformOnly(spark: SparkSession = null, config: CryptoShreddingConfig = null)
    extends CryptoShredding(spark, config) {

    val SaltTest = "test-salt"

    override def getWorkspaceConfig(spark: SparkSession, config: CryptoShreddingConfig): WorkspaceConfig = {
        WorkspaceConfig(Map())
    }

    override def getSalt(workspaceConfig: WorkspaceConfig): String = SaltTest

    override def upsertToPrivacyTable(
        extractedPersonalInformationDf: Dataset[CryptoHashingResult],
        spark: SparkSession,
        config: CryptoShreddingConfig,
        workspaceConfig: WorkspaceConfig
    ): Unit = {
        // NOP
    }
}

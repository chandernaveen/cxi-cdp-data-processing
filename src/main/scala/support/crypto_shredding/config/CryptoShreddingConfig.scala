package com.cxi.cdp.data_processing
package support.crypto_shredding.config

import support.packages.utils.ContractUtils

case class CryptoShreddingConfig
    (
        country: String,
        cxiPartnerId: String,
        lookupDestDbName: String,
        lookupDestTableName: String,
        workspaceConfigPath: String
    )

object CryptoShreddingConfig {
    def apply(contractUtils: ContractUtils): CryptoShreddingConfig = {
        CryptoShreddingConfig(
            country = contractUtils.prop[String]("partner.country"),
            cxiPartnerId = contractUtils.prop[String]("partner.cxiPartnerId"),
            lookupDestDbName = contractUtils.prop[String]("schema.crypto.db_name"),
            lookupDestTableName = contractUtils.prop[String]("schema.crypto.lookup_table"),
            workspaceConfigPath = contractUtils.prop[String]("databricks_workspace_config")
        )
    }
}

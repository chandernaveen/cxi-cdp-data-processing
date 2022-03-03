package com.cxi.cdp.data_processing
package support.crypto_shredding.config

case class CryptoShreddingConfig
    (
        country: String,
        cxiSource: String,
        lookupDestDbName: String,
        lookupDestTableName: String,
        workspaceConfigPath: String
    )

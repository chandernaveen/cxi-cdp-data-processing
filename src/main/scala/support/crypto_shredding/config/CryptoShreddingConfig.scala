package com.cxi.cdp.data_processing
package support.crypto_shredding.config

import com.cxi.cdp.data_processing.support.normalization.DateNormalization

import java.time.LocalDate

case class CryptoShreddingConfig(
    country: String,
    cxiSource: String,
    date: LocalDate,
    runId: String,
    lookupDestDbName: String,
    lookupDestTableName: String,
    workspaceConfigPath: String
) {
    require(country != null && country.trim.nonEmpty, "country should not be empty")
    require(cxiSource != null && cxiSource.trim.nonEmpty, "cxiSource should not be empty")
    require(date != null, "date should not be null")
    require(runId != null && runId.trim.nonEmpty, "runId should not be empty")
    require(lookupDestDbName != null && lookupDestDbName.trim.nonEmpty, "lookupDestDbName should not be empty")
    require(lookupDestTableName != null && lookupDestTableName.trim.nonEmpty, "lookupDestTableName should not be empty")
    require(workspaceConfigPath != null && workspaceConfigPath.trim.nonEmpty, "workspaceConfigPath should not be empty")

    def dateRaw: String = DateNormalization.formatFromLocalDate(date).get
}

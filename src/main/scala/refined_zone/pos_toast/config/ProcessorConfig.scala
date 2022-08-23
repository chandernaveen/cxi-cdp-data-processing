package com.cxi.cdp.data_processing
package refined_zone.pos_toast.config

import support.normalization.DateNormalization
import support.utils.ContractUtils

import java.time.LocalDate

case class ProcessorConfig(
    contract: ContractUtils,
    date: LocalDate,
    cxiPartnerId: String,
    runId: String,
    srcDbName: String,
    srcTable: String
) {
    require(contract != null, "contract should not be null")
    require(date != null, "date should not be null")
    require(cxiPartnerId != null && cxiPartnerId.trim.nonEmpty, "cxiPartnerId should not be empty")
    require(runId != null && runId.trim.nonEmpty, "runId should not be empty")
    require(srcDbName != null && srcDbName.trim.nonEmpty, "srcDbName should not be empty")
    require(srcTable != null && srcTable.trim.nonEmpty, "srcTable should not be empty")

    def dateRaw: String = DateNormalization.formatFromLocalDate(date).get
}

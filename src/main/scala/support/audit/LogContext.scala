package com.cxi.cdp.data_processing
package support.audit

case class LogContext(
    logTable: String = "testing.application_audit_logs",
    processName: String,
    entity: String,
    runID: Int,
    dpYear: Int,
    dpMonth: Int,
    dpDay: Int,
    dpHour: Int,
    dpPartition: String = null,
    subEntity: String = null,
    processStartTime: String = null,
    processEndTime: String = null,
    writeStatus: String = null,
    errorMessage: String = "NA"
)

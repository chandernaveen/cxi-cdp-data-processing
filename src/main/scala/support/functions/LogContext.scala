package com.cxi.cdp.data_processing
package support.functions

class LogContext(val logTable: String = "testing.application_audit_logs",
                 val processName: String,
                 val entity: String,
                 val runID: Int,
                 dpYearStr: String,
                 dpMonthStr: String,
                 dpDayStr: String,
                 dpHourStr: String,
                 val dpPartition: String = null,
                 val subEntity: String = null,
                 val processStartTime: String = null,
                 val processEndTime: String = null,
                 val writeStatus: String = null,
                 val errorMessage: String = "NA"
                ) {
    val dpYear: Int = toInt(dpYearStr)
    val dpMonth: Int = toInt(dpMonthStr)
    val dpDay: Int = toInt(dpDayStr)
    val dpHour: Int = toInt(dpHourStr)

    def toInt(x: String): Int = {
        if (x.trim.nonEmpty) x.trim.toInt else 0
    }
}

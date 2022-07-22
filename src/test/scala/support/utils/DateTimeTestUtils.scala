package com.cxi.cdp.data_processing
package support.utils

object DateTimeTestUtils {

    def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }

    def sqlTimestamp(dateTime: String): java.sql.Timestamp = {
        java.sql.Timestamp.from(java.time.ZonedDateTime.parse(dateTime).toInstant)
    }

}

package com.cxi.cdp.data_processing
package support.normalization

import java.time.LocalDate
import java.time.format.DateTimeFormatter

sealed trait Normalization

case object DateNormalization extends Normalization {

    final val STANDARD_DATE_FORMAT: String = "yyyy-MM-dd"

    def parseToLocalDate(date: String, pattern: String = STANDARD_DATE_FORMAT): LocalDate = {
        LocalDate.parse(date, DateTimeFormatter.ofPattern(pattern))
    }

    def formatFromLocalDate(date: LocalDate): Option[String] = {
        Some(date).map(_.format(DateTimeFormatter.ofPattern(STANDARD_DATE_FORMAT)))
    }
}

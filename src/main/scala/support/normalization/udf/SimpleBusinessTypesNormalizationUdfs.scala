package com.cxi.cdp.data_processing
package support.normalization.udf

import support.normalization._

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, udf}
import org.apache.spark.sql.Column

sealed trait SimpleBusinessTypesNormalizationUdfs extends NormalizationUdfs

case object TimestampNormalizationUdfs extends SimpleBusinessTypesNormalizationUdfs {

    /** Uses ISO-like date-time formatter that formats or parses a date-time with the offset and zone if available,
      * such as '2011-12-03T10:15:30', '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30+01:00[Europe/Paris]'.
      */
    def parseToTimestampIsoDateTime: UserDefinedFunction =
        udf((value: String) => TimestampNormalization.parseToTimestamp(value))

    /** Converts a column to timestamp data type column with ability to provide the format as well.
      */
    def parseToTimestamp(col: Column, format: String = null): Column = {
        Option(format).map(f => to_timestamp(col, f)).getOrElse(to_timestamp(col))
    }

    /** Uses custom pattern (e.g. 'yyy.MM.dd'T'HH.mm.ssZ') and custom timezone (e.g. "+02:00", "UTC" etc.)
      * to parse not ISO8601-compliant strings to the timestamp.
      * See underlying [[com.cxi.cdp.data_processing.support.normalization.TimestampNormalization.parseToTimestamp]]
      * for more details.
      */
    def parseToTimestampWithPatternAndTimezone: UserDefinedFunction =
        udf((value: String, pattern: Option[String], timeZone: Option[String]) =>
            TimestampNormalization.parseToTimestamp(value, pattern, timeZone)
        )
}

case object DateNormalizationUdfs extends SimpleBusinessTypesNormalizationUdfs {

    /** Uses the standard ISO date formatter that formats or parses a date, such as '2011-12-03'.
      */
    def parseToSqlDateIsoFormat: UserDefinedFunction = udf((value: String) => DateNormalization.parseToSqlDate(value))

    /** Uses custom pattern (e.g. 'yyyy.MM.dd') to parse not ISO8601-compliant strings to the date
      */
    def parseToSqlDateWithPattern: UserDefinedFunction =
        udf((value: String, pattern: String) => DateNormalization.parseToSqlDate(value, pattern))

    /** Converts a column to date data type with ability to provide the format as well.
      * For example allows converting timestamp data type columns.
      */
    def parseToSqlDate(col: Column, format: String = null): Column = {
        Option(format).map(f => to_date(col, f)).getOrElse(to_date(col))
    }
}

case object LocationNormalizationUdfs extends SimpleBusinessTypesNormalizationUdfs {
    def normalizeZipCode: UserDefinedFunction = udf(LocationNormalization.normalizeZipCode _)
}

case object MoneyNormalizationUdfs extends SimpleBusinessTypesNormalizationUdfs {

    private final val CXI_MONEY_FORMAT = "decimal(9,2)"

    def convertCentsToMoney(columnName: String): Column =
        udf((cents: String) => MoneyNormalization.convertCentsToMoney(cents))
            .apply(col(columnName))
            .cast(CXI_MONEY_FORMAT)
}

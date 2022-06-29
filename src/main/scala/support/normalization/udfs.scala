package com.cxi.cdp.data_processing
package support.normalization

import support.normalization.LocationNormalization.MoneyNormalization

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.Column

sealed trait NormalizationUdfs extends Serializable

case object TimestampNormalizationUdfs extends NormalizationUdfs {

    /** Uses ISO-like date-time formatter that formats or parses a date-time with the offset and zone if available,
      * such as '2011-12-03T10:15:30', '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30+01:00[Europe/Paris]'.
      */
    def parseToTimestampIsoDateTime: UserDefinedFunction =
        udf((value: String) => TimestampNormalization.parseToTimestamp(value))

    /** Uses custom pattern (e.g. 'yyy.MM.dd'T'HH.mm.ssZ') to parse not ISO8601-compliant strings to the timestamp
      */
    def parseToTimestampWithPattern: UserDefinedFunction =
        udf((value: String, pattern: Option[String]) => TimestampNormalization.parseToTimestamp(value, pattern))
}

case object DateNormalizationUdfs extends NormalizationUdfs {

    /** Uses the standard ISO date formatter that formats or parses a date, such as '2011-12-03'.
      */
    def parseToSqlDateIsoFormat: UserDefinedFunction = udf((value: String) => DateNormalization.parseToSqlDate(value))

    /** Uses custom pattern (e.g. 'yyyy.MM.dd') to parse not ISO8601-compliant strings to the date
      */
    def parseToSqlDateWithPattern: UserDefinedFunction =
        udf((value: String, pattern: String) => DateNormalization.parseToSqlDate(value, pattern))
}

case object LocationNormalizationUdfs extends NormalizationUdfs {
    def normalizeZipCode: UserDefinedFunction = udf(LocationNormalization.normalizeZipCode _)
}

case object MoneyNormalizationUdfs extends NormalizationUdfs {

    private final val CXI_MONEY_FORMAT = "decimal(9,2)"

    def convertCentsToMoney(columnName: String): Column =
        udf((cents: String) => MoneyNormalization.convertCentsToMoney(cents))
            .apply(col(columnName))
            .cast(CXI_MONEY_FORMAT)
}

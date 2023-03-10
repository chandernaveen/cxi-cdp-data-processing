package com.cxi.cdp.data_processing
package support.normalization

import com.google.i18n.phonenumbers.{NumberParseException, PhoneNumberUtil}
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat
import org.apache.commons.validator.routines.EmailValidator
import org.apache.log4j.Logger

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime._

sealed trait SimpleBusinessTypesNormalization extends Normalization

case object DateNormalization extends SimpleBusinessTypesNormalization {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val STANDARD_DATE_FORMAT: String = "yyyy-MM-dd"

    def parseToLocalDate(date: String, pattern: String = STANDARD_DATE_FORMAT): LocalDate = {
        LocalDate.parse(date, DateTimeFormatter.ofPattern(pattern))
    }

    def formatFromLocalDate(date: LocalDate): Option[String] = {
        Some(date).map(_.format(DateTimeFormatter.ofPattern(STANDARD_DATE_FORMAT)))
    }

    /** If custom pattern is not provided, uses ISO-like date format: 'yyyy-MM-dd'
      */
    def parseToSqlDate(date: String, pattern: String = STANDARD_DATE_FORMAT): Option[java.sql.Date] = {
        try {
            Some(java.sql.Date.valueOf(parseToLocalDate(date, pattern)))
        } catch {
            case _: Exception =>
                logger.warn(s"Cannot parse to sqlDate: '$date' with pattern '$pattern'")
                None
        }
    }
}

case object TimestampNormalization extends SimpleBusinessTypesNormalization {

    private val logger = Logger.getLogger(this.getClass.getName)

    /** Converts a date-time string to the instance of [[java.sql.Timestamp]]
      *
      * @param timestamp the date-time string to convert
      * @param pattern custom pattern (optional).
      * If custom pattern is not provided, uses ISO-like date-time formatter that formats or parses a date-time
      * with the offset and zone if available, such as '2011-12-03T10:15:30+01:00'
      * or '2011-12-03T10:15:30+01:00[Europe/Paris]'.
      * @param timeZone custom timezone (optional).
      * If timestamp does not contain timezone information, this parameters must be set as [[java.time.Instant]]
      * cannot be formatted as a date or time without providing some form of time-zone,
      * see [[https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_INSTANT]]
      * Takes precedence over timezone specified in the timestamp string.
      * @return an instance of [[java.sql.Timestamp]]
      */
    def parseToTimestamp(
        timestamp: String,
        pattern: Option[String] = None,
        timeZone: Option[String] = None
    ): Option[Timestamp] = {

        try {
            val formatter = pattern match {
                case Some(value) => DateTimeFormatter.ofPattern(value)
                case None => DateTimeFormatter.ISO_DATE_TIME
            }
            val temporalAccessor = formatter.parse(timestamp)

            val instant = timeZone match {
                case Some(tz) => LocalDateTime.from(temporalAccessor).atZone(ZoneId.of(tz)).toInstant
                case None => Instant.from(temporalAccessor)
            }
            Some(Timestamp.from(instant))
        } catch {
            case _: RuntimeException =>
                logger.warn(s"Cannot parse to timestamp: '$timestamp' with pattern '$pattern'")
                None
        }
    }

    /** Convert epoch/unix time in second to timestamp
      */
    def convertToTimestamp(epoch: Long): Option[Timestamp] = {
        try {
            val instant = Instant.ofEpochSecond(epoch)
            Some(Timestamp.from(instant))
        } catch {
            case _: RuntimeException =>
                logger.warn(s"Cannot convert from epoch: '$epoch' to timestamp")
                None
        }
    }

    def parseTimeStampToLTC(
        timestamp: Timestamp,
        timeZone: Option[String] = None,
        pattern: Option[String] = None
    ): Option[Timestamp] = {

        try {
            /*   val formatter = pattern match {
                case Some(value) => DateTimeFormatter.ofPattern(value)
                case None => DateTimeFormatter.ISO_DATE_TIME
            }
          //  val temporalAccessor = formatter.parse(timestamp)*/
            val timeZoneInstant = timeZone match {
                case Some(tz) => tz
                case None => "UTC"
            }

            val instant = timestamp.toInstant

            val local_instant = ofInstant(instant, ZoneId.of(timeZoneInstant))

            Some(Timestamp.valueOf(local_instant))

        } catch {

            case _: RuntimeException =>
                logger.warn(s"Cannot parse to timestamp: '$timestamp' with pattern '$pattern'")
                None
        }

    }

}

case object EmailNormalization extends SimpleBusinessTypesNormalization {

    private final val EmailValidatorInstance = EmailValidator.getInstance()

    def normalizeEmail(value: String): Option[String] = {
        Option(value)
            .map(_.trim)
            .filter(_.nonEmpty)
            .filter(email => EmailValidatorInstance.isValid(email))
            .map(_.toLowerCase)
    }
}

case object PhoneNumberNormalization extends SimpleBusinessTypesNormalization {

    private val logger = Logger.getLogger(this.getClass.getName)

    private final val PhoneNumberUtilInstance = PhoneNumberUtil.getInstance()

    def normalizePhoneNumber(value: String): Option[String] = {
        try {
            Option(value)
                .map(_.trim)
                .filter(_.nonEmpty)
                .map(value => PhoneNumberUtilInstance.parse(value, "US"))
                .filter(phoneNumber => PhoneNumberUtilInstance.isValidNumber(phoneNumber))
                .map(phoneNumber => PhoneNumberUtilInstance.format(phoneNumber, PhoneNumberFormat.E164))
                // remove leading '+'
                .map(_.substring(1))
        } catch {
            case _: NumberParseException =>
                logger.warn(s"Cannot parse phone number: '$value'")
                None
        }
    }
}

case object AdvertiserIdNormalization extends SimpleBusinessTypesNormalization {

    def normalizeAdvertiserId(id: String): Option[String] = {
        Option(id)
            .map(_.trim)
            .filter(_.nonEmpty)
            .map(_.toUpperCase)
    }

}

case object LocationNormalization extends SimpleBusinessTypesNormalization {

    private final val zipCodeRegex = raw"(\d{5})".r
    private final val zipPlusFourCodeRegex = raw"(\d{5})-(\d{4})".r // ZIP+4 Code format

    def normalizeZipCode(zipCode: String): Option[String] = {
        zipCode match {
            case zipCodeRegex(cleansedZipCode) => Some(cleansedZipCode)
            case zipPlusFourCodeRegex(cleansedZipCode, _) => Some(cleansedZipCode)
            case _ => None
        }
    }

    def locationSpecialCharacters(city: String): String = {
        StringContext.processEscapes(city).filter(_ >= ' ')
    }

}

case object MoneyNormalization extends SimpleBusinessTypesNormalization {

    private val logger = Logger.getLogger(this.getClass.getName)

    private final val CentsPerDollar = 100

    /** Converts cents to dollars, e.g. "12345" -> 213.45, "500" -> 5.00
      * @param cents integer value in string format, e.g. "12345", "500" etc.
      * @return floating point (BigDecimal) dollars amount
      */
    def convertCentsToMoney(cents: String): Option[java.math.BigDecimal] = {
        try {
            Option(cents)
                .map(_.trim)
                .filter(_.nonEmpty)
                .map(v => new java.math.BigDecimal(v))
                .map(v => v.divide(new java.math.BigDecimal(CentsPerDollar)))
        } catch {
            case _: NumberFormatException =>
                logger.warn(s"Cannot convert cents value to money format: '$cents'")
                None
        }
    }

}

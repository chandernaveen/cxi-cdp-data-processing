package com.cxi.cdp.data_processing
package support.normalization

import com.google.i18n.phonenumbers.{NumberParseException, PhoneNumberUtil}
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat
import org.apache.commons.validator.routines.EmailValidator
import org.apache.log4j.Logger

import java.sql.Timestamp
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

sealed trait Normalization

case object DateNormalization extends Normalization {

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
                logger.warn(s"Cannot parse to sqlDate: '$date'")
                None
        }
    }
}

case object TimestampNormalization extends Normalization {

    private val logger = Logger.getLogger(this.getClass.getName)

    /** If custom pattern is not provided, uses ISO-like date-time formatter that formats or parses a date-time
      * with the offset and zone if available, such as '2011-12-03T10:15:30', '2011-12-03T10:15:30+01:00'
      * or '2011-12-03T10:15:30+01:00[Europe/Paris]'.
      */
    def parseToTimestamp(timestamp: String, pattern: Option[String] = None): Option[Timestamp] = {
        try {
            val formatter = pattern match {
                case Some(value) => DateTimeFormatter.ofPattern(value)
                case None => DateTimeFormatter.ISO_DATE_TIME
            }
            val temporalAccessor = formatter.parse(timestamp)
            val instant = Instant.from(temporalAccessor)
            Some(Timestamp.from(instant))
        } catch {
            case _: RuntimeException =>
                logger.warn(s"Cannot parse to timestamp: '$timestamp'")
                None
        }
    }
}

case object EmailNormalization extends Normalization {

    private final val EmailValidatorInstance = EmailValidator.getInstance()

    def normalizeEmail(value: String): Option[String] = {
        Option(value)
            .map(_.trim)
            .filter(_.nonEmpty)
            .filter(email => EmailValidatorInstance.isValid(email))
            .map(_.toLowerCase)
    }
}

case object PhoneNumberNormalization extends Normalization {

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

case object AdvertiserIdNormalization extends Normalization {

    def normalizeAdvertiserId(id: String): Option[String] = {
        Option(id)
            .map(_.trim)
            .filter(_.nonEmpty)
            .map(_.toUpperCase)
    }

}

case object LocationNormalization extends Normalization {

    private final val zipCodeRegex = raw"(\d{5})".r
    private final val zipPlusFourCodeRegex = raw"(\d{5})-(\d{4})".r // ZIP+4 Code format

    def normalizeZipCode(zipCode: String): Option[String] = {
        zipCode match {
            case zipCodeRegex(cleansedZipCode) => Some(cleansedZipCode)
            case zipPlusFourCodeRegex(cleansedZipCode, _) => Some(cleansedZipCode)
            case _ => None
        }
    }

    case object MoneyNormalization extends Normalization {

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

}

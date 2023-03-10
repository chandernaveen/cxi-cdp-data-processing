package com.cxi.cdp.data_processing
package support.normalization

import support.normalization.AdvertiserIdNormalization.normalizeAdvertiserId
import support.normalization.DateNormalization.STANDARD_DATE_FORMAT
import support.normalization.EmailNormalization.normalizeEmail
import support.normalization.PhoneNumberNormalization.normalizePhoneNumber

import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

import java.sql.Timestamp
import java.sql.Timestamp.from
import java.time.{LocalDate, ZoneId}
import java.time.LocalDateTime.of
import java.time.ZoneOffset.UTC

class SimpleBusinessTypesNormalizationTest extends FunSuite {

    test("Normalize phone number") {
        val phone_numbers = Seq(
            "2124567890",
            "212-456-7890",
            "(212)456-7890",
            "(212)-456-7890",
            "212.456.7890",
            "212 456 7890",
            "+12124567890",
            "+12124567890",
            "+1 212.456.7890",
            "1-212-456-7890",
            "12124567890",
            "  12124567890  "
        )
        for (phone_number <- phone_numbers) {
            normalizePhoneNumber(phone_number) shouldBe Some("12124567890")
        }
    }

    test("Invalid phone number - normalization should return None") {
        val phone_numbers =
            Seq(null, "", " ", "123456789", "!@!$#", "123--123--1234", "+212-456-7890", "1ABC2124567890")
        for (phone_number <- phone_numbers) {
            normalizePhoneNumber(phone_number) shouldBe None
        }
    }

    test("Normalize email") {
        val emails = Seq(
            "paul123@mailbox.com",
            "PAUL123@mailbox.com",
            "Paul123@mailBoX.com",
            "Paul123@Mailbox.Com",
            " Paul123@Mailbox.Com "
        )
        for (email <- emails) {
            normalizeEmail(email) shouldBe Some("paul123@mailbox.com")
        }
    }

    test("Invalid email - normalization should return None") {
        val emails = Seq(null, "some@host", "", "@domain.com", " ", "123@123.123")
        for (email <- emails) {
            normalizeEmail(email) shouldBe None
        }
    }

    test("Normalize advertiser id") {
        val maids = Seq(
            "ABCD1234-EF56-GH78-IJ90-KLMO1234PQRS",
            "abcd1234-ef56-gh78-ij90-klmo1234pqrs",
            "Abcd1234-Ef56-Gh78-iJ90-klmO1234pqrS",
            "Abcd1234-Ef56-Gh78-iJ90-klmO1234pqrS",
            "  Abcd1234-Ef56-Gh78-iJ90-klmO1234pqrS  "
        )
        for (maid <- maids) {
            normalizeAdvertiserId(maid) shouldBe Some("ABCD1234-EF56-GH78-IJ90-KLMO1234PQRS")
        }
    }

    test("Empty advertiser id - normalization should return None") {
        val maids = Seq("", null, " ", "   ")
        for (maid <- maids) {
            normalizeAdvertiserId(maid) shouldBe None
        }
    }

    test("Normalize timestamp") {
        val testCases = Seq(
            TimestampNormalizationTestCase(
                "2021-11-02T23:59:16Z",
                None,
                None,
                Some(from(of(2021, 11, 2, 23, 59, 16).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "2022-02-24T04:30:00.000Z",
                None,
                None,
                Some(from(of(2022, 2, 24, 4, 30, 0, 0).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "2021-05-13T21:50:55.435Z",
                None,
                None,
                Some(from(of(2021, 5, 13, 21, 50, 55, 435000000).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "2022-12-03T11:15:30+02:00",
                None, // +02:00 timezone
                None,
                Some(from(of(2022, 12, 3, 9, 15, 30).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "20220224T07.15.00+0300",
                Some("yyyyMMdd'T'HH.mm.ssZ"), // custom pattern with +03:00 timezone
                None,
                Some(from(of(2022, 2, 24, 4, 15, 0).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "2020-04-07T11:48:58.553",
                Some("yyyy-MM-dd'T'HH:mm:ss.SSS"), // custom pattern without TZ
                Some("America/Los_Angeles"),
                Some(from(of(2020, 4, 7, 18, 48, 58, 553000000).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "20220224T13.15.00+0200",
                Some("yyyyMMdd'T'HH.mm.ssZ"), // custom pattern with +02:00 timezone
                Some("America/Los_Angeles"), // explicitly set 'America/Los_Angeles' zone
                Some(from(of(2022, 2, 24, 21, 15, 0).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase(
                "2022-07-07 09:41:32 America/Los_Angeles",
                Some("yyyy-MM-dd HH:mm:ss VV"), // custom pattern with named timezone
                None,
                Some(from(of(2022, 7, 7, 16, 41, 32).toInstant(UTC)))
            ),
            TimestampNormalizationTestCase("some invalid string", None, None, None),
            TimestampNormalizationTestCase("", None, None, None),
            TimestampNormalizationTestCase(null, None, None, None)
        )
        for (testcase <- testCases) {
            TimestampNormalization.parseToTimestamp(
                testcase.value,
                testcase.pattern,
                testcase.timeZone
            ) shouldBe testcase.expected
        }
    }

    test("Convert to LTC from UTC") {

        val testCases = Seq(
            TimestampNormalizationLTCTestCase(
                java.sql.Timestamp.valueOf("2022-04-30 10:10:00"),
                None,
                Some("America/Los_Angeles"),
                Some(java.sql.Timestamp.valueOf("2022-04-30 3:10:00"))
            ),
            TimestampNormalizationLTCTestCase(
                from(of(2021, 2, 24, 6, 30, 0, 0).atZone(UTC).toInstant),
                None,
                None,
                Some(from(of(2021, 2, 24, 6, 30, 0, 0).atZone(UTC).toInstant))
            ),
            TimestampNormalizationLTCTestCase(null, None, None, None)
        )
        for (testcase <- testCases) {
            println(
                "actual" + TimestampNormalization.parseTimeStampToLTC(
                    testcase.value,
                    testcase.timeZone,
                    testcase.pattern
                )
            )
            println("Expected" + testcase.expected)
            println("sent" + testcase.value)
            TimestampNormalization.parseTimeStampToLTC(
                testcase.value,
                testcase.timeZone,
                testcase.pattern
            ) shouldBe testcase.expected
        }
    }

    test("Normalize sql date") {
        val testCases = Seq(
            SqlDateNormalizationTestCase(
                "2022-02-24",
                STANDARD_DATE_FORMAT,
                Some(java.sql.Date.valueOf(LocalDate.of(2022, 2, 24)))
            ),
            SqlDateNormalizationTestCase(
                "2022.02.24",
                "yyyy.MM.dd",
                Some(java.sql.Date.valueOf(LocalDate.of(2022, 2, 24)))
            ), // custom pattern
            SqlDateNormalizationTestCase("some invalid string", STANDARD_DATE_FORMAT, None),
            SqlDateNormalizationTestCase("", STANDARD_DATE_FORMAT, None),
            SqlDateNormalizationTestCase(null, STANDARD_DATE_FORMAT, None)
        )
        for (testcase <- testCases) {
            DateNormalization.parseToSqlDate(testcase.value, testcase.pattern) shouldBe testcase.expected
        }
    }

    test("Normalize zip code") {
        val testCases = Seq(
            ZipCodeNormalizationTestCase("12345", Some("12345")),
            ZipCodeNormalizationTestCase("98765-4321", Some("98765")),
            ZipCodeNormalizationTestCase("some invalid string", None),
            ZipCodeNormalizationTestCase("ZIP Code: 98765-4321", None),
            ZipCodeNormalizationTestCase("1234", None),
            ZipCodeNormalizationTestCase("", None),
            ZipCodeNormalizationTestCase(null, None)
        )
        for (testcase <- testCases) {
            LocationNormalization.normalizeZipCode(testcase.value) shouldBe testcase.expected
        }
    }

    test("Convert cents to money") {
        val testCases = Seq(
            MoneyNormalizationTestCase("12345", Some(java.math.BigDecimal.valueOf(123.45))),
            MoneyNormalizationTestCase("", None),
            MoneyNormalizationTestCase("some invalid string", None),
            MoneyNormalizationTestCase(null, None)
        )
        for (testcase <- testCases) {
            MoneyNormalization.convertCentsToMoney(testcase.value) shouldBe testcase.expected
        }
    }

    case class TimestampNormalizationTestCase(
        value: String,
        pattern: Option[String],
        timeZone: Option[String],
        expected: Option[Timestamp]
    )

    case class TimestampNormalizationLTCTestCase(
        value: Timestamp,
        pattern: Option[String],
        timeZone: Option[String],
        expected: Option[Timestamp]
    )

    case class SqlDateNormalizationTestCase(value: String, pattern: String, expected: Option[java.sql.Date])
    case class ZipCodeNormalizationTestCase(value: String, expected: Option[String])
    case class MoneyNormalizationTestCase(value: String, expected: Option[java.math.BigDecimal])

}

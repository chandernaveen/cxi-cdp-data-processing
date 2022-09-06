package com.cxi.cdp.data_processing
package support.normalization.udf

import support.normalization.udf.DateNormalizationUdfs.{parseToSqlDateIsoFormat, parseToSqlDateWithPattern}
import support.normalization.udf.LocationNormalizationUdfs.normalizeZipCode
import support.normalization.udf.MoneyNormalizationUdfs.convertCentsToMoney
import support.normalization.udf.TimestampNormalizationUdfs.{
    parseToTimestampIsoDateTime,
    parseToTimestampWithPatternAndTimezone
}
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.math.RoundingMode
import java.sql.Timestamp.from
import java.time.LocalDate
import java.time.LocalDateTime.of
import java.time.ZoneOffset.UTC

class SimpleBusinessTypesNormalizationUdfsTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("parseToTimestamp udf with Iso DateTime formatted") {

        // given
        val df = Seq(
            ("id_1", "2021-05-13T21:50:55.435Z"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "timestamp")

        // when
        val actual = df.select(col("id"), parseToTimestampIsoDateTime(col("timestamp")).as("timestamp"))

        // then
        withClue("timestamp is not correctly parsed from string") {
            val expected = Seq(
                ("id_1", from(of(2021, 5, 13, 21, 50, 55, 435000000).toInstant(UTC))),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "timestamp")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("parseToTimestamp with custom pattern udf") {

        // given
        val df = Seq(
            ("id_1", "2022.02.24T07.15.00+0000"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "timestamp")

        // when
        val actual = df.select(
            col("id"),
            parseToTimestampWithPatternAndTimezone(col("timestamp"), lit("yyyy.MM.dd'T'HH.mm.ssZ"), lit(null)).as(
                "timestamp"
            )
        )

        // then
        withClue("timestamp is not correctly parsed from string") {
            val expected = Seq(
                ("id_1", from(of(2022, 2, 24, 7, 15, 0).toInstant(UTC))),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "timestamp")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("parseToTimestamp with custom pattern and timezone udf") {

        // given
        val df = Seq(
            ("id_1", "2022.02.24T07.15.00"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "timestamp")

        // when
        val actual = df.select(
            col("id"),
            parseToTimestampWithPatternAndTimezone(col("timestamp"), lit("yyyy.MM.dd'T'HH.mm.ss"), lit("-02:00")).as(
                "timestamp"
            )
        )

        // then
        withClue("timestamp is not correctly parsed from string") {
            val expected = Seq(
                ("id_1", from(of(2022, 2, 24, 9, 15, 0).toInstant(UTC))),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "timestamp")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("parseToSqlDate with standard pattern udf") {

        // given
        val df = Seq(
            ("id_1", "2022-02-24"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "date")

        // when
        val actual = df.select(col("id"), parseToSqlDateIsoFormat(col("date")).as("date"))

        // then
        withClue("sql date is not correctly parsed from string") {
            val expected = Seq(
                ("id_1", LocalDate.of(2022, 2, 24)),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "date")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

    test("parseToSqlDate with custom pattern udf") {

        // given
        val df = Seq(
            ("id_1", "2022.02.24"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "date")

        // when
        val actual = df.select(col("id"), parseToSqlDateWithPattern(col("date"), lit("yyyy.MM.dd")).as("date"))

        // then
        withClue("sql date is not correctly parsed from string") {
            val expected = Seq(
                ("id_1", LocalDate.of(2022, 2, 24)),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "date")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

    test("normalizeZipCode udf") {

        // given
        val df = Seq(
            ("id_1", "98765-4321"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "zipcode")

        // when
        val actual = df.select(col("id"), normalizeZipCode(col("zipcode")).as("zipcode"))

        // then
        withClue("zip code is not correctly normalized") {
            val expected = Seq(
                ("id_1", "98765"),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "zipcode")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

    test("convert cents to money udf") {

        // given
        val df = Seq(
            ("id_1", "12345"),
            ("id_2", null),
            ("id_3", ""),
            ("id_4", "some string")
        ).toDF("id", "amount")

        // when
        val actual = df.withColumn("amount", convertCentsToMoney("amount"))

        // then
        withClue("cents not correctly converted to money") {
            val expected = Seq(
                ("id_1", new java.math.BigDecimal(123.45).setScale(2, RoundingMode.HALF_EVEN)),
                ("id_2", null),
                ("id_3", null),
                ("id_4", null)
            ).toDF("id", "amount")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

}

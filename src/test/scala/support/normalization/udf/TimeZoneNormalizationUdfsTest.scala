package com.cxi.cdp.data_processing
package support.normalization.udf

import support.normalization.udf.TimeZoneNormalizationUdfs.normalizeTimezone
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class TimeZoneNormalizationUdfsTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("check valid timezones") {

        // given
        val df = Seq(
            ("id_1", "Egypt"), // one worded zone
            ("id_2", "America/Fort_Nelson"), // zone with underscore-char
            ("id_3", "US/Pacific"), // regular zone spec
            ("id_4", "Africa/Porto-Novo"), // zone with special char
            ("Id_5", "America/Argentina/Buenos_Aires"), // zone-triplet
            ("Id_6", "America/North_Dakota/New_Salem"), // zone triplet with special chars
            ("id_7", "GMT+2") // zone but with time-offset
        ).toDF("id", "tzone")

        // when
        val actual = df.select(
            col("id"),
            normalizeTimezone(col("tzone")).as("tzone")
        )

        // then
        withClue("time zone is not correctly parsed from string") {
            val expected = Seq(
                ("id_1", "Egypt"),
                ("id_2", "America/Fort_Nelson"),
                ("id_3", "US/Pacific"),
                ("id_4", "Africa/Porto-Novo"),
                ("Id_5", "America/Argentina/Buenos_Aires"),
                ("Id_6", "America/North_Dakota/New_Salem"),
                ("id_7", "+02:00")
            ).toDF("id", "tzone")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("check non-existing timezone that is lexically correct ") {

        // given
        val df = Seq(
            ("id_1", "Abc/Xyz") // no such zone
        ).toDF("id", "tzone")

        // then
        withClue("time zone with offset aren't valid") {
            assertThrows[Exception]({
                df.select(
                    col("id"),
                    normalizeTimezone(col("tzone")).as("tzone")
                ).collect()
            })
        }
    }

    test("check correct timezone that has wrong case ") {

        // given
        val df = Seq(
            ("id_1", "america/los_angeles") // zone correct but low-case
        ).toDF("id", "tzone")

        // then
        withClue("time zone with offset aren't valid") {
            assertThrows[Exception]({
                df.select(
                    col("id"),
                    normalizeTimezone(col("tzone")).as("tzone")
                ).collect()
            })
        }
    }
    test("check no nulls allowed") {

        // given
        val df = Seq(
            ("id_1", null) // zone correct but low-case
        ).toDF("id", "tzone")

        // then
        withClue("time zone with offset aren't valid") {
            assertThrows[Exception]({
                df.select(
                    col("id"),
                    normalizeTimezone(col("tzone")).as("tzone")
                ).collect()
            })
        }
    }

}

package com.cxi.cdp.data_processing
package refined_zone.throtle
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class RawRefinedThrotleEmailJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("read throtle email") {

        // given
        val feedDate = "2022-03-31"

        val rawTable = "tid_email"
        val tid_email_raw = Seq(
            (
                "throtle_id_1",
                "throtle_hhid_11",
                "email_hash_11",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "978a28d5-e445-4e7f-8ea0-41858de9d90f"
            ),
            (
                "throtle_id_1",
                null,
                "email_hash_12",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "eccd77d2-8d2b-4f29-886d-4b90d196ee54"
            ),
            (
                "throtle_id_1",
                "throtle_hhid_12",
                null,
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "fa61f698-2cd1-442b-bcd9-a85284706f86"
            ),
            (
                "throtle_id_1",
                null,
                null,
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "2c1c2094-68b5-4fae-87e9-4727e17479eb"
            ),
            (
                "throtle_id_1",
                "throtle_hhid_11",
                "email_hash_13",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "035af5b5-d8b8-48d7-b9d3-6bb21b5462c2"
            ),
            (
                "throtle_id_2",
                "throtle_hhid_21",
                "email_hash_21",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "ff2935e1-3469-421a-a6f2-3e7bff427dd4"
            ),
            (
                "throtle_id_2",
                "throtle_hhid_22",
                null,
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "a75fb793-611b-45f4-b325-e1c3d42a64ae"
            )
        ).toDF("throtle_id", "throtle_hhid", "sha256_lower_email", "feed_date", "file_name", "cxi_id")
        tid_email_raw.createOrReplaceTempView(rawTable)

        // when
        val actual = RawRefinedThrotleEmailJob.readThrotleEmailRaw(rawTable, feedDate, spark)

        // then
        withClue("Raw tid_email data based on feed_date") {
            val expected = Seq(
                ("throtle_id_1", "throtle_hhid_11", "email_hash_11"),
                ("throtle_id_1", null, "email_hash_12"),
                ("throtle_id_1", "throtle_hhid_11", "email_hash_13"),
                ("throtle_id_2", "throtle_hhid_21", "email_hash_21")
            ).toDF("throtle_id", "throtle_hhid", "sha256_lower_email")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Transform raw Email data") {

        // given
        val tid_email_raw = Seq(
            ("throtle_id_1", "throtle_hhid_11", "email_hash_11"),
            ("throtle_id_1", null, "email_hash_12"),
            ("throtle_id_1", "throtle_hhid_11", "email_hash_13"),
            ("throtle_id_2", "throtle_hhid_21", "email_hash_21")
        ).toDF("throtle_id", "throtle_hhid", "sha256_lower_email")

        // when
        val actual = RawRefinedThrotleEmailJob.transformThrotleEmail(tid_email_raw)

        // then
        withClue("Transformed tid_Email data on Group by") {
            val rawStruct = new StructType()
                .add("throtle_id", StringType)
                .add("throtle_hhid", StringType)
                .add("sha256_lower_emails", DataTypes.createArrayType(StringType, false), false)

            val expectedRaw = Seq(
                Row("throtle_id_1", "throtle_hhid_11", Array("email_hash_11", "email_hash_13", "email_hash_12")),
                Row("throtle_id_2", "throtle_hhid_21", Array("email_hash_21"))
            )
            import collection.JavaConverters._
            val expected = spark.createDataFrame(expectedRaw.asJava, rawStruct)

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }
}

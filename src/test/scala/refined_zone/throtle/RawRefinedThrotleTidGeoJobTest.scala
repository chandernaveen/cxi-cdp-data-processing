package com.cxi.cdp.data_processing
package refined_zone.throtle
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, the}

class RawRefinedThrotleTidGeoJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("read throtle geo") {

        // given
        val feedDate = "2022-06-30"

        val rawTable = "tid_geo"
        val tid_geo_raw = Seq(
            (
                "throtle_id_1",
                null,
                "21060",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "978a28d5-e445-4e7f-8ea0-41858de9d90f"
            ),
            (
                "throtle_id_2",
                "383165779 ",
                "94925",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "eccd77d2-8d2b-4f29-886d-4b90d196ee54"
            ),
            (
                "throtle_id_3",
                null,
                null,
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "fa61f698-2cd1-442b-bcd9-a85284706f86"
            ),
            (
                "throtle_id_1",
                "375274822",
                "14103",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "2c1c2094-68b5-4fae-87e9-4727e17479eb"
            ),
            (
                "throtle_id_1",
                null,
                "21060",
                feedDate,
                "part-00093-5aa8ea92-c9fc-463e-bc2e-1a2bed9e5cf9-c000.csv.gz",
                "978a28d5-e445-4e7f-8ea0-41858de9d90f"
            )
        ).toDF("throtle_id", "throtle_hhid", "zip", "feed_date", "file_name", "cxi_id")
        tid_geo_raw.createOrReplaceTempView(rawTable)

        // when
        val actual = RawRefinedThrotleTidGeoJob.readThrotleGeoRaw(rawTable, feedDate, spark)

        // then
        withClue("Raw tid_geo data based on feed_date") {
            val expected = Seq(
                ("throtle_id_1", null, "21060"),
                ("throtle_id_2", "383165779 ", "94925")
            ).toDF("throtle_id", "throtle_hhid", "zip")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Transform raw Geo data") {

        // given
        val tid_geo_raw = Seq(
            ("throtle_id_1", "383165779", "94925-4321"),
            ("throtle_id_2", null, "14103"),
            ("throtle_id_3", "299649563", "06234")
        ).toDF("throtle_id", "throtle_hhid", "zip")

        // when
        val actual = RawRefinedThrotleTidGeoJob.transformThrotleGeo(tid_geo_raw)

        // then
        withClue("Transformed tid_Geo data ignoring duplicates") {
            val rawStruct = new StructType()
                .add("throtle_id", StringType)
                .add("throtle_hhid", StringType)
                .add("zip_code", StringType, false)

            val expectedRaw = Seq(
                Row("throtle_id_1", "383165779", "94925"),
                Row("throtle_id_2", null, "14103"),
                Row("throtle_id_3", "299649563", "06234")
            )
            import collection.JavaConverters._
            val expected = spark.createDataFrame(expectedRaw.asJava, rawStruct)

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }
}

package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import refined_zone.hub.geo_identity.model.GeoLocationRow
import support.utils.DateTimeTestUtils._
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterEach, Matchers}

import scala.collection.JavaConverters._

class GeoLocationOverlapJobTest extends BaseSparkBatchJobTest with Matchers with BeforeAndAfterEach {

    import GeoLocationOverlapJob._
    import GeoLocationOverlapJobTest._

    override def beforeAll(): Unit = {
        super.beforeAll()
        setupSedona(spark)
    }

    test("getDateFilter for full reprocess") {
        getDateFilter(feedDate = "2022-06-10", fullReprocess = true) shouldBe "true"
    }

    test("getDateFilter for incremental processing") {
        getDateFilter(feedDate = "2022-06-10", fullReprocess = false) shouldBe "feed_date = '2022-06-10'"
    }

    test("readLocations") {
        // given
        val rawStruct = new StructType()
            .add("cxi_partner_id", StringType)
            .add("location_id", StringType)
            .add("lat", StringType)
            .add("long", StringType)
            .add("active_flg", StringType)

        val rawData = Seq(
            Row("cxi-usa-goldbowl", "L9DVPH030DDQV", "36.104479", "-115.136573", "1"),
            Row("cxi-usa-sluttyvegan", "4", "33.743437403462025", "-84.43798749630737", "1"),
            Row("cxi-usa-goldbowl", "LH79HBQ892WC7", "36.195522", "-115.248131", "1"),
            Row("cxi-usa-goldbowl", "LH79HBQ892WC7", "36.195522", "-115.248131", "0"),
            Row("cxi-usa-goldbowl", "LH79HBQ892WC7", "", "", "1")
        )

        val dfInput = spark.createDataFrame(rawData.asJava, rawStruct)
        val srcTable = "testLocation"
        dfInput.createOrReplaceTempView(srcTable)

        // when
        val actual = readLocations(srcTable)(spark)

        // then
        val expected = spark
            .createDataFrame(
                List(
                    ("cxi-usa-goldbowl", "L9DVPH030DDQV", "36.104479", "-115.136573"),
                    ("cxi-usa-sluttyvegan", "4", "33.743437403462025", "-84.43798749630737"),
                    ("cxi-usa-goldbowl", "LH79HBQ892WC7", "36.195522", "-115.248131")
                )
            )
            .toDF("cxi_partner_id", "location_id", "lat", "long")

        assertDataFrameNoOrderEquals(expected, actual)
    }

    test("getStoreLocations") {
        // given
        val rawStruct = new StructType()
            .add("cxi_partner_id", StringType)
            .add("location_id", StringType)
            .add("lat", StringType)
            .add("long", StringType)

        val rawData = Seq(
            Row("cxi-usa-goldbowl", "L9DVPH030DDQV", "36.104479", "-115.136573"),
            Row("cxi-usa-sluttyvegan", "4", "33.743437403462025", "-84.43798749630737"),
            Row("cxi-usa-goldbowl", "LH79HBQ892WC7", "36.195522", "-115.248131")
        )

        val dfInput = spark.createDataFrame(rawData.asJava, rawStruct)

        // when
        val actual = getStoreLocationsInMeters(dfInput)(spark)

        // then
        val expected = spark
            .createDataFrame(
                List(
                    (
                        "cxi-usa-goldbowl",
                        "L9DVPH030DDQV",
                        -12816944.67804257,
                        4315007.060857129
                    ),
                    (
                        "cxi-usa-sluttyvegan",
                        "4",
                        -9399593.771697737,
                        3994403.7443555705
                    ),
                    (
                        "cxi-usa-goldbowl",
                        "LH79HBQ892WC7",
                        -12829363.257796485,
                        4327558.356661824
                    )
                )
            )
            .toDF("cxi_partner_id", "location_id", "store_location_meters_x", "store_location_meters_y")
            .withColumn("store_location_meters", expr("ST_POINT(store_location_meters_x, store_location_meters_y)"))
            .drop("store_location_meters_x", "store_location_meters_y")

        assertDataFrameNoOrderEquals(expected, actual)
    }

    test("getGeoLocationOverlap") {
        import spark.implicits._

        // given
        val horizontalAccuracyMax = 15.0
        val distanceToStore = 10.0
        val accuracyMultiplier = 1.0

        val verasetDf = List(
            // horizontal_accuracy is too high - filtered out before join
            VerasetRow(
                utc_timestamp = "2022-06-10T00:01:15Z",
                latitude = 38.00002,
                longitude = -122.00003,
                horizontal_accuracy = 17.0,
                id_type = "id_type_1",
                advertiser_id_AAID = "advertiser_id_aaid_1",
                advertiser_id_IDFA = null
            ),
            // matches location_1
            VerasetRow(
                utc_timestamp = "2022-06-12T00:02:10Z",
                latitude = 38.00002,
                longitude = -122.00003,
                horizontal_accuracy = 3.5,
                id_type = "id_type_2",
                advertiser_id_AAID = "advertiser_id_aaid_2",
                advertiser_id_IDFA = null
            ),
            // matches location_1 and location_2
            VerasetRow(
                utc_timestamp = "2022-06-09T23:20:00Z",
                latitude = 38.00001,
                longitude = -121.99997,
                horizontal_accuracy = 9.5,
                id_type = "id_type_3",
                advertiser_id_AAID = null,
                advertiser_id_IDFA = "advertiser_id_idfa_3"
            ),
            // too far from our locations
            VerasetRow(
                utc_timestamp = "2022-06-09T21:45:00Z",
                latitude = 37.9,
                longitude = -121.9,
                horizontal_accuracy = 3.0,
                id_type = "id_type_4",
                advertiser_id_AAID = null,
                advertiser_id_IDFA = "advertiser_id_idfa_4"
            )
        ).toDF

        val storeLocations = spark
            .createDataFrame(
                List(
                    // lat = 38.0, lon = -122.0
                    (
                        "partner_1",
                        "location_1",
                        -13580977.88,
                        4579425.81
                    ),
                    // lat = 38.0, lon = -121.99985
                    (
                        "partner_2",
                        "location_2",
                        -13580961.18,
                        4579425.81
                    )
                )
            )
            .toDF("cxi_partner_id", "location_id", "store_location_meters_x", "store_location_meters_y")
            .withColumn("store_location_meters", expr("ST_POINT(store_location_meters_x, store_location_meters_y)"))
            .drop("store_location_meters_x", "store_location_meters_y")

        // when
        val actual = getGeoLocationOverlap(
            horizontalAccuracyMax = horizontalAccuracyMax,
            distanceToStore = distanceToStore,
            accuracyMultiplier = accuracyMultiplier,
            storeLocationsInMeters = storeLocations,
            veraset = verasetDf
        )(spark)

        // then
        val expected = List(
            GeoLocationRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                maid = "advertiser_id_aaid_2",
                maid_type = "id_type_2",
                latitude = 38.00002,
                longitude = -122.00003,
                horizontal_accuracy = 3.5,
                geo_timestamp = sqlTimestamp("2022-06-12T00:02:10Z"),
                geo_date = sqlDate("2022-06-12"),
                distance_to_store = 4.373790004180821,
                max_distance_to_store = 13.5
            ),
            GeoLocationRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                maid = "advertiser_id_idfa_3",
                maid_type = "id_type_3",
                latitude = 38.00001,
                longitude = -121.99997,
                horizontal_accuracy = 9.5,
                geo_timestamp = sqlTimestamp("2022-06-09T23:20:00Z"),
                geo_date = sqlDate("2022-06-09"),
                distance_to_store = 3.6301634120640482,
                max_distance_to_store = 19.5
            ),
            GeoLocationRow(
                cxi_partner_id = "partner_2",
                location_id = "location_2",
                maid = "advertiser_id_idfa_3",
                maid_type = "id_type_3",
                latitude = 38.00001,
                longitude = -121.99997,
                horizontal_accuracy = 9.5,
                geo_timestamp = sqlTimestamp("2022-06-09T23:20:00Z"),
                geo_date = sqlDate("2022-06-09"),
                distance_to_store = 13.431991207326577,
                max_distance_to_store = 19.5
            )
        ).toDF

        // not using `assertDataFrameNoOrderEquals` because of different column nullability inferred by Spark
        assertDataFrameDataEquals(expected, actual)
        actual.schema.fieldNames shouldBe expected.schema.fieldNames
    }

}

object GeoLocationOverlapJobTest {

    private[GeoLocationOverlapJobTest] case class VerasetRow(
        utc_timestamp: String, // unix time
        latitude: Double,
        longitude: Double,
        horizontal_accuracy: Double,
        id_type: String,
        advertiser_id_AAID: String,
        advertiser_id_IDFA: String
    )

    private[GeoLocationOverlapJobTest] def parseAsUnixTime(dateTime: String) = {
        java.time.ZonedDateTime.parse(dateTime).toEpochSecond
    }
}

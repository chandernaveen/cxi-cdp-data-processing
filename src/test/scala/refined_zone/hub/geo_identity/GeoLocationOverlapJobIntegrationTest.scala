package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import refined_zone.hub.geo_identity.model.GeoLocationRow
import support.tags.RequiresDatabricksRemoteCluster
import support.utils.DateTimeTestUtils._
import support.BaseSparkBatchJobTest

import org.scalatest.{BeforeAndAfterEach, Matchers}

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class GeoLocationOverlapJobIntegrationTest extends BaseSparkBatchJobTest with Matchers with BeforeAndAfterEach {

    import GeoLocationOverlapJob._

    private val geoLocationDestTable = generateUniqueTableName("integration_test_geo_location")

    test("writeGeoLocation") {
        import spark.implicits._

        createGeoLocationTable(geoLocationDestTable)
        try {
            // first test - with an empty table

            val firstBatch = Seq(
                GeoLocationRow(
                    cxi_partner_id = "partner_1",
                    location_id = "location_1",
                    maid = "advertiser_id_aaid_1",
                    maid_type = "id_type_1",
                    latitude = 38.00002,
                    longitude = -122.00003,
                    horizontal_accuracy = 3.5,
                    geo_timestamp = sqlTimestamp("2022-06-12T00:02:10Z"),
                    geo_date = sqlDate("2022-06-12"),
                    distance_to_store = 4.37,
                    max_distance_to_store = 13.5
                )
            ).toDF

            writeGeoLocation(firstBatch, geoLocationDestTable, fullReprocess = false)(spark)

            assertDataFrameDataEquals(firstBatch, spark.table(geoLocationDestTable))

            // second test - with a table that has an existing record

            val secondBatch = Seq(
                // update existing record
                GeoLocationRow(
                    cxi_partner_id = "partner_1",
                    location_id = "location_1",
                    maid = "advertiser_id_aaid_1",
                    maid_type = "id_type_1",
                    latitude = 37.5,
                    longitude = -121.0,
                    horizontal_accuracy = 4.0,
                    geo_timestamp = sqlTimestamp("2022-06-12T00:02:10Z"),
                    geo_date = sqlDate("2022-06-12"),
                    distance_to_store = 4.9,
                    max_distance_to_store = 12.5
                ),
                // add new record
                GeoLocationRow(
                    cxi_partner_id = "partner_2",
                    location_id = "location_2",
                    maid = "advertiser_id_aaid_2",
                    maid_type = "id_type_2",
                    latitude = 35.0,
                    longitude = -117.0,
                    horizontal_accuracy = 7.0,
                    geo_timestamp = sqlTimestamp("2022-06-10T02:00:40Z"),
                    geo_date = sqlDate("2022-06-10"),
                    distance_to_store = 6.2,
                    max_distance_to_store = 11.0
                )
            ).toDF

            writeGeoLocation(secondBatch, geoLocationDestTable, fullReprocess = false)(spark)

            assertDataFrameDataEquals(secondBatch, spark.table(geoLocationDestTable))

            // third test - full reprocess write should delete old data

            val thirdBatch = Seq(
                GeoLocationRow(
                    cxi_partner_id = "partner_3",
                    location_id = "location_3",
                    maid = "advertiser_id_aaid_3",
                    maid_type = "id_type_3",
                    latitude = 32.0,
                    longitude = -110.0,
                    horizontal_accuracy = 10.0,
                    geo_timestamp = sqlTimestamp("2022-06-10T04:00:40Z"),
                    geo_date = sqlDate("2022-06-10"),
                    distance_to_store = 7.0,
                    max_distance_to_store = 10.0
                )
            ).toDF

            writeGeoLocation(thirdBatch, geoLocationDestTable, fullReprocess = true)(spark)

            assertDataFrameDataEquals(thirdBatch, spark.table(geoLocationDestTable))
        } finally {
            dropTable(geoLocationDestTable)
        }
    }

    private def createGeoLocationTable(tableName: String): Unit = {
        spark.sql(s"""
            CREATE TABLE IF NOT EXISTS `$tableName` (
              `cxi_partner_id` STRING,
              `location_id` STRING,
              `maid` STRING,
              `maid_type` STRING,
              `latitude` DOUBLE,
              `longitude` DOUBLE,
              `horizontal_accuracy` DOUBLE,
              `geo_timestamp` TIMESTAMP,
              `geo_date` DATE,
              `distance_to_store` DOUBLE,
              `max_distance_to_store` DOUBLE
            )
            USING delta
            PARTITIONED BY (cxi_partner_id, geo_date)
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
       """)
    }

    private def dropTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

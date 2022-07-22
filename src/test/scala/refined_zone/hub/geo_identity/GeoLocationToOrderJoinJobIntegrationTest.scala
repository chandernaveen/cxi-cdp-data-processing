package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import refined_zone.hub.geo_identity.model.GeoLocationToOrderRow
import refined_zone.hub.identity.model.IdentityId
import support.tags.RequiresDatabricksRemoteCluster
import support.utils.DateTimeTestUtils.sqlDate
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class GeoLocationToOrderJoinJobIntegrationTest extends BaseSparkBatchJobTest with Matchers {

    import GeoLocationToOrderJoinJob._

    private val geoLocationToOrderDestTable = generateUniqueTableName("integration_test_geo_location_to_order")

    test("writeGeoLocation") {
        import spark.implicits._

        createGeoLocationToOrderTable(geoLocationToOrderDestTable)
        try {
            // first test - write to an empty table

            val batchForJune10th = Seq(
                GeoLocationToOrderRow(
                    cxi_partner_id = "partner_1",
                    location_id = "location_1",
                    ord_id = "order_id_1",
                    ord_date = sqlDate("2022-06-10"),
                    cxi_identity_ids = Seq(IdentityId("phone", "111")),
                    maid = "maid_1",
                    maid_type = "maid_type_1",
                    device_score = Some(0.5)
                )
            ).toDF

            writeGeoLocationToOrder(
                batchForJune10th,
                geoLocationToOrderDestTable,
                replaceWhereCondition = Some("`ord_date` IN ('2022-06-10')")
            )(spark)

            assertDataFrameDataEquals(batchForJune10th, spark.table(geoLocationToOrderDestTable))

            // second test - write to a non-emtpy table with an empty cleanup condition

            val batchForJune12th = Seq(
                GeoLocationToOrderRow(
                    cxi_partner_id = "partner_2",
                    location_id = "location_2",
                    ord_id = "order_id_2",
                    ord_date = sqlDate("2022-06-12"),
                    cxi_identity_ids = Seq(IdentityId("phone", "222")),
                    maid = "maid_2",
                    maid_type = "maid_type_2",
                    device_score = Some(1.0)
                )
            ).toDF

            writeGeoLocationToOrder(
                batchForJune12th,
                geoLocationToOrderDestTable,
                replaceWhereCondition = Some("`ord_date` IN ('2022-06-12')")
            )(spark)

            assertDataFrameDataEquals(
                batchForJune10th.union(batchForJune12th),
                spark.table(geoLocationToOrderDestTable)
            )

            // third test - overwrite specific day

            val updatedBatchForJune10th = Seq(
                GeoLocationToOrderRow(
                    cxi_partner_id = "partner_1_updated",
                    location_id = "location_1_updated",
                    ord_id = "order_id_1_updated",
                    ord_date = sqlDate("2022-06-10"),
                    cxi_identity_ids = Seq(IdentityId("phone", "111-222"), IdentityId("email", "someone@example.com")),
                    maid = "maid_1_updated",
                    maid_type = "maid_type_1_updated",
                    device_score = Some(0.7)
                )
            ).toDF

            writeGeoLocationToOrder(
                updatedBatchForJune10th,
                geoLocationToOrderDestTable,
                replaceWhereCondition = Some("`ord_date` IN ('2022-06-10')")
            )(spark)

            assertDataFrameDataEquals(
                updatedBatchForJune10th.union(batchForJune12th),
                spark.table(geoLocationToOrderDestTable)
            )

            // fourth test - write to a non-emtpy table with an empty replace condition == overwrite everything

            val batchForJune15th = Seq(
                GeoLocationToOrderRow(
                    cxi_partner_id = "partner_3",
                    location_id = "location_3",
                    ord_id = "order_id_3",
                    ord_date = sqlDate("2022-06-15"),
                    cxi_identity_ids = Seq(IdentityId("phone", "333")),
                    maid = "maid_3",
                    maid_type = "maid_type_3",
                    device_score = Some(0.2)
                )
            ).toDF

            writeGeoLocationToOrder(batchForJune15th, geoLocationToOrderDestTable, replaceWhereCondition = None)(spark)

            assertDataFrameDataEquals(batchForJune15th, spark.table(geoLocationToOrderDestTable))
        } finally {
            dropTable(geoLocationToOrderDestTable)
        }
    }

    private def createGeoLocationToOrderTable(tableName: String): Unit = {
        spark.sql(s"""
            CREATE TABLE IF NOT EXISTS `$tableName` (
              `cxi_partner_id` STRING,
              `location_id` STRING,
              `ord_id` STRING,
              `ord_date` DATE,
              `cxi_identity_ids` ARRAY<STRUCT<`identity_type`: STRING, `cxi_identity_id`: STRING>>,
              `maid` STRING,
              `maid_type` STRING,
              `device_score` DOUBLE)
            USING delta
            PARTITIONED BY (ord_date);
       """)
    }

    private def dropTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

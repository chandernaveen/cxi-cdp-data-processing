package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import refined_zone.hub.geo_identity.model.{GeoLocationRow, GeoLocationToOrderRow}
import refined_zone.hub.identity.model.IdentityId
import support.utils.DateTimeTestUtils.{sqlDate, sqlTimestamp}
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers

import java.time.LocalDate

class GeoLocationToOrderJoinJobTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._
    import GeoLocationToOrderJoinJob._
    import GeoLocationToOrderJoinJobTest._

    test("getFinalOrderAndGeoLocationDates for empty order and geo location dates") {
        getFinalOrderAndGeoLocationDates(
            changedOrderDates = Set.empty,
            changedGeoLocationDates = Set.empty
        ) shouldBe (
            Set.empty,
            Set.empty
        )
    }

    test("getFinalOrderAndGeoLocationDates for empty geo location dates") {
        getFinalOrderAndGeoLocationDates(
            changedOrderDates = Set(LocalDate.parse("2022-06-10")),
            changedGeoLocationDates = Set.empty
        ) shouldBe (
            Set(LocalDate.parse("2022-06-10")),
            Set(LocalDate.parse("2022-06-09"), LocalDate.parse("2022-06-10"), LocalDate.parse("2022-06-11"))
        )
    }

    test("getFinalOrderAndGeoLocationDates for empty order dates") {
        getFinalOrderAndGeoLocationDates(
            changedOrderDates = Set.empty,
            changedGeoLocationDates = Set(LocalDate.parse("2022-06-10"))
        ) shouldBe (
            Set(LocalDate.parse("2022-06-09"), LocalDate.parse("2022-06-10"), LocalDate.parse("2022-06-11")),
            Set(
                LocalDate.parse("2022-06-08"),
                LocalDate.parse("2022-06-09"),
                LocalDate.parse("2022-06-10"),
                LocalDate.parse("2022-06-11"),
                LocalDate.parse("2022-06-12")
            )
        )
    }

    test("getFinalOrderAndGeoLocationDates for non-empty dates") {
        getFinalOrderAndGeoLocationDates(
            changedOrderDates = Set(LocalDate.parse("2022-06-07")),
            changedGeoLocationDates = Set(LocalDate.parse("2022-06-10"))
        ) shouldBe (
            Set(
                LocalDate.parse("2022-06-07"),
                LocalDate.parse("2022-06-09"),
                LocalDate.parse("2022-06-10"),
                LocalDate.parse("2022-06-11")
            ),
            Set(
                LocalDate.parse("2022-06-06"),
                LocalDate.parse("2022-06-07"),
                LocalDate.parse("2022-06-08"),
                LocalDate.parse("2022-06-09"),
                LocalDate.parse("2022-06-10"),
                LocalDate.parse("2022-06-11"),
                LocalDate.parse("2022-06-12")
            )
        )
    }

    test("getReplaceWhereConditionForOrderDates") {
        val orderDates = Seq(LocalDate.parse("2022-06-10"), LocalDate.parse("2022-06-15"))
        val expectedResult = "`ord_date` IN ('2022-06-10', '2022-06-15')"
        getReplaceWhereConditionForOrderDates(orderDates) shouldBe expectedResult
    }

    test("joinOrdersWithGeoLocation") {
        val timestampJoinInterval = "INTERVAL 5 MINUTES"

        val orderSummaryRows = Seq(
            OrderSummaryRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_timestamp = sqlTimestamp("2022-06-10T00:02:35Z"),
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111"))
            ),
            // no matching cxi_partner_id in geo location data
            OrderSummaryRow(
                cxi_partner_id = "partner_2",
                location_id = "location_1",
                ord_id = "order_id_2",
                ord_timestamp = sqlTimestamp("2022-06-10T00:02:35Z"),
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111"))
            ),
            OrderSummaryRow(
                cxi_partner_id = "partner_3",
                location_id = "location_3",
                ord_id = "order_id_3",
                ord_timestamp = sqlTimestamp("2022-06-15T23:57:20Z"),
                ord_date = sqlDate("2022-06-15"),
                cxi_identity_ids = Seq(IdentityId("phone", "333"))
            )
        )

        val geoLocationRows = Seq(
            // matches order_id_1
            GeoLocationRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                maid = "maid_1",
                maid_type = "maid_type_1",
                latitude = 38.0,
                longitude = -122.0,
                horizontal_accuracy = 3.5,
                geo_timestamp = sqlTimestamp("2022-06-10T00:06:44Z"),
                geo_date = sqlDate("2022-06-10"),
                distance_to_store = 4.5,
                max_distance_to_store = 13.5
            ),
            // matches order_id_1 from the following day
            GeoLocationRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                maid = "maid_2",
                maid_type = "maid_type_2",
                latitude = 38.02,
                longitude = -122.02,
                horizontal_accuracy = 4.5,
                geo_timestamp = sqlTimestamp("2022-06-09T23:59:23Z"),
                geo_date = sqlDate("2022-06-09"),
                distance_to_store = 6.0,
                max_distance_to_store = 13.5
            ),
            // no match with order_id_1 as cxi_timestamp is outside of `timestampJoinInterval`
            GeoLocationRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                maid = "maid_3",
                maid_type = "maid_type_3",
                latitude = 38.04,
                longitude = -122.04,
                horizontal_accuracy = 3.0,
                geo_timestamp = sqlTimestamp("2022-06-10T00:07:36Z"),
                geo_date = sqlDate("2022-06-10"),
                distance_to_store = 4.0,
                max_distance_to_store = 13.5
            ),
            // matches order_id_3 from the previous day
            GeoLocationRow(
                cxi_partner_id = "partner_3",
                location_id = "location_3",
                maid = "maid_4",
                maid_type = "maid_type_4",
                latitude = 38.08,
                longitude = -121.92,
                horizontal_accuracy = 8.0,
                geo_timestamp = sqlTimestamp("2022-06-16T00:02:00Z"),
                geo_date = sqlDate("2022-06-16"),
                distance_to_store = 7.5,
                max_distance_to_store = 13.5
            )
        )

        val expectedResult = Seq(
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_1",
                maid_type = "maid_type_1"
            ),
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_2",
                maid_type = "maid_type_2"
            ),
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_3",
                location_id = "location_3",
                ord_id = "order_id_3",
                ord_date = sqlDate("2022-06-15"),
                cxi_identity_ids = Seq(IdentityId("phone", "333")),
                maid = "maid_4",
                maid_type = "maid_type_4"
            )
        )

        val resultDf =
            joinGeoLocationWithOrder(orderSummaryRows.toDF, geoLocationRows.toDF, timestampJoinInterval)(spark)

        assertDataFrameNoOrderEquals(expectedResult.toDF, resultDf)
    }

    test("addDeviceScore") {
        val ordersWithGeoLocationRows = Seq(
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_1",
                maid_type = "maid_type_1"
            ),
            // same ord_id / cxi_partner_id as the previous record
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_2",
                maid_type = "maid_type_2"
            ),
            // same ord_id as before but different cxi_partner_id
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_2",
                location_id = "location_2",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_2",
                maid_type = "maid_type_2"
            ),
            IntermediateGeoLocationToOrderRow(
                cxi_partner_id = "partner_3",
                location_id = "location_3",
                ord_id = "order_id_3",
                ord_date = sqlDate("2022-06-15"),
                cxi_identity_ids = Seq(IdentityId("phone", "333")),
                maid = "maid_4",
                maid_type = "maid_type_4"
            )
        )

        val expectedResult = Seq(
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_1",
                maid_type = "maid_type_1",
                device_score = Some(0.5)
            ),
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_2",
                maid_type = "maid_type_2",
                device_score = Some(0.5)
            ),
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_2",
                location_id = "location_2",
                ord_id = "order_id_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "111")),
                maid = "maid_2",
                maid_type = "maid_type_2",
                device_score = Some(1.0)
            ),
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_3",
                location_id = "location_3",
                ord_id = "order_id_3",
                ord_date = sqlDate("2022-06-15"),
                cxi_identity_ids = Seq(IdentityId("phone", "333")),
                maid = "maid_4",
                maid_type = "maid_type_4",
                device_score = Some(1.0)
            )
        )

        val resultDf = addDeviceScore(ordersWithGeoLocationRows.toDF)(spark)

        assertDataFrameNoOrderEquals(expectedResult.toDF, resultDf)
    }

}

object GeoLocationToOrderJoinJobTest {

    // only the fields we need for testing
    private[GeoLocationToOrderJoinJobTest] case class OrderSummaryRow(
        cxi_partner_id: String,
        location_id: String,
        ord_id: String,
        ord_timestamp: java.sql.Timestamp,
        ord_date: java.sql.Date,
        cxi_identity_ids: Seq[IdentityId]
    )

    private[GeoLocationToOrderJoinJobTest] case class IntermediateGeoLocationToOrderRow(
        cxi_partner_id: String,
        location_id: String,
        ord_id: String,
        ord_date: java.sql.Date,
        cxi_identity_ids: Seq[IdentityId],
        maid: String,
        maid_type: String
    )

}

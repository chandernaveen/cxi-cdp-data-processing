package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, PickupDetails}
import refined_zone.hub.model.ChannelType
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.scalatest.Matchers

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {

    import OrderSummaryProcessorTest._

    test("getOrdTargetChannelId") {
        import Fulfillment.{State, Type}
        import spark.implicits._

        val protoFulfillment = Fulfillment(pickup_details = PickupDetails(note = "a note"), state = "FL")
        val unknownType = "some unknown type"

        val inputDF = List[OrdTargetChannelIdTestCase](
            OrdTargetChannelIdTestCase(fulfillments = null, expectedOrdTargetChannelId = ChannelType.Unknown.code),
            OrdTargetChannelIdTestCase(fulfillments = Seq(), expectedOrdTargetChannelId = ChannelType.Unknown.code),
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(protoFulfillment.copy(`type` = Type.Pickup)),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code
            ),

            // prefer COMPLETED fulfillment
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = unknownType, state = State.Proposed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Completed)
                ),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code
            ),

            // COMPLETED fulfillment has null type, so ignore it
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = null, state = State.Completed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)
                ),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code
            ),
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = unknownType, state = State.Completed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)
                ),
                expectedOrdTargetChannelId = ChannelType.Unknown.code
            ),
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(protoFulfillment.copy(`type` = unknownType)),
                expectedOrdTargetChannelId = ChannelType.Unknown.code
            )
        ).toDS

        val results = inputDF
            .withColumn("actualOrdTargetChannelId", OrderSummaryProcessor.getOrdTargetChannelId(col("fulfillments")))
            .collect

        results.foreach { result =>
            val fulfillments = result.getAs[Seq[Fulfillment]]("fulfillments")
            val expectedOrdTargetChannelId = result.getAs[Int]("expectedOrdTargetChannelId")
            val actualOrdTargetChannelId = result.getAs[Int]("actualOrdTargetChannelId")
            withClue(s"actual channel type id does not match for fulfillments: $fulfillments") {
                actualOrdTargetChannelId shouldBe expectedOrdTargetChannelId
            }
        }
    }

    test("coalesceClosedAt") {
        import spark.implicits._

        val baseDate = Instant.now()
        val defaultTimeZone = ZoneId.systemDefault()

        val dtFormatShort = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(defaultTimeZone)
        val dtFormatLong = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(defaultTimeZone)
        val dateNow = dtFormatShort.format(baseDate)

        val past15 = dtFormatLong.format(baseDate.minusSeconds(15))
        val past20 = dtFormatLong.format(baseDate.minusSeconds(20))

        val sharedId1 = 42
        val sharedId2 = 75
        val tableName = "raw_orders"
        implicit val formats: DefaultFormats = DefaultFormats
        val listData = List[RawDataEmulator](
            // case for normal flow
            RawDataEmulator(
                dateNow,
                Serialization.write(new OrderSummaryFromRaw(ord_id = sharedId1, opened_at = past15, closed_at = past20))
            ),
            // case for BUG check
            RawDataEmulator(
                dateNow,
                Serialization.write(new OrderSummaryFromRaw(ord_id = sharedId2, opened_at = past20, closed_at = null))
            )
        )
        val inputDF = listData.toDF
        inputDF.createGlobalTempView(tableName)

        val results = OrderSummaryProcessor.readOrderSummary(
            spark, dateNow, "global_temp", tableName).collect

        results.foreach { result =>
            // convert back to date
            val ordDate = result.getAs[String]("ord_date")
            val orderTimestamp = result.getAs[String]("ord_timestamp")
            withClue(s"all records must be closed at: $past20") {
                ordDate shouldBe past20
            }
            withClue(s"all records timestamp must be closed at: $past20") {
                orderTimestamp shouldBe past20
            }
        }
    }
}

object OrderSummaryProcessorTest {
    case class OrdTargetChannelIdTestCase(fulfillments: Seq[Fulfillment], expectedOrdTargetChannelId: Int)

    case class OrderSummaryFromRaw(
        ord_id: Int,
        opened_at: String,
        closed_at: String,
        ord_date: String,
        ord_timestamp: String,
        ord_total: Float,
        discount_amount: Float,
        fulfillments: String,
        line_items: String,
        service_charge_amount: Int,
        total_taxes_amount: Float,
        total_tip_amount: Float,
        customer_id: String,
        tender_array: String,
        location_id: String,
        ord_state: String,
        discount_id: String,
        cxi_partner_id: String,
        record_type: String,
        feed_date: String
    ) {
        def this(ord_id: Int, opened_at: String, closed_at: String) = this(
            ord_id = ord_id,
            opened_at = opened_at,
            closed_at = closed_at,
            ord_date = opened_at,
            ord_timestamp = opened_at,
            ord_total = 1081,
            discount_amount = 0,
            fulfillments = "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
            line_items = "[]",
            service_charge_amount = 0,
            total_taxes_amount = 10,
            total_tip_amount = 5,
            customer_id = null,
            tender_array = "[]",
            location_id = "dsdsd",
            ord_state = "COMPLETED",
            discount_id = null,
            cxi_partner_id = "AABBCC",
            record_type = "orders",
            feed_date = opened_at
        )
    }
    case class RawDataEmulator(feed_date: String, record_value: String, record_type: String = "orders")

}

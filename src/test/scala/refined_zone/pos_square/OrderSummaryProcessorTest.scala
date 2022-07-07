package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, PickupDetails}
import refined_zone.hub.model.ChannelType
import support.BaseSparkBatchJobTest

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.scalatest.Matchers

import java.time.{Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {
    private val logger = Logger.getLogger(classOf[OrderSummaryProcessorTest].getName)

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
    test("testRead") {
        import spark.implicits._
        val tableName = "raw_orders"
        // given
        val baseDate = Instant.now()
        val dtFormatShort = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())
        val dateNow = "2022-07-05"
        val rawOrderSummary = List(
            (
                dateNow,
                """{
                  "id":"orderId1","created_at":"2022-07-05T12:42Z","closed_at":"2022-07-05T12:42Z","total_money":{"amount":224},"fulfillments":"[{'pickup_details':{'note':'abc'}, 'state':'FL'}]","line_items":"[{'catalog_object_id':1, 'quantity':2}]","total_service_charge_money":{"amount":703},"total_tax_money":{"amount":10},"total_tip_money":{"amount":5},"total_discount_money":{"amount":1},"customer_id":"ABC-123","tenders":"{'id':42}","location_id":"dsdsd","state":"COMPLETED","discounts":{"uid":"uidABC"}
                  }""",
                "orders"
            ),
            (
                dateNow,
                """{
                   "id":"orderId2","created_at":"2022-07-05T12:42Z","closed_at":null,"total_money":{"amount":45},"fulfillments":"[{'pickup_details':{'note':'abc'}, 'state':'FL'}]","line_items":"[{'catalog_object_id':1, 'quantity':2}]","total_service_charge_money":{"amount":746},"total_tax_money":{"amount":10},"total_tip_money":{"amount":5},"total_discount_money":{"amount":1},"customer_id":"XYZ:789","tenders":"{'id':17}","location_id":"dsdsd","state":"COMPLETED","discounts":{"uid":"uidXYZ"}
                   }""",
                "orders"
            )
        ).toDF("feed_date", "record_value", "record_type")

        // GlobalTempView used since it allows place data
        // to context where SQL works as "SELECT * FROM global_temp.tableName"
        rawOrderSummary.createOrReplaceGlobalTempView(tableName)

        // when
        val orderSummaryActual = OrderSummaryProcessor.readOrderSummary(spark, dateNow, "global_temp", tableName)
        // then
        val actualFieldsReturned = orderSummaryActual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + orderSummaryActual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "ord_id",
                "ord_total",
                "discount_amount",
                "created_at",
                "ord_date",
                "ord_timestamp",
                "location_id",
                "ord_state",
                "fulfillments",
                "discount_id",
                "line_items",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "customer_id",
                "tender_array"
            )
        }
        val actualSquareLocationsData = orderSummaryActual.collect

        withClue("read JSON order summary does not match") {
            val expected = List(
                (
                    "orderId1",
                    "224",
                    "1",
                    "2022-07-05T12:42Z",
                    "2022-07-05T12:42Z",
                    "2022-07-05T12:42Z",
                    "dsdsd",
                    "COMPLETED",
                    "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
                    "uidABC",
                    "[{'catalog_object_id':1, 'quantity':2}]",
                    "703",
                    "10",
                    "5",
                    "ABC-123",
                    "{'id':42}"
                ),
                (
                    "orderId2",
                    "45",
                    "1",
                    "2022-07-05T12:42Z",
                    null,
                    null,
                    "dsdsd",
                    "COMPLETED",
                    "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
                    "uidXYZ",
                    "[{'catalog_object_id':1, 'quantity':2}]",
                    "746",
                    "10",
                    "5",
                    "XYZ:789",
                    "{'id':17}"
                )
            ).toDF(
                "ord_id",
                "ord_total",
                "discount_amount",
                "created_at",
                "ord_date",
                "ord_timestamp",
                "location_id",
                "ord_state",
                "fulfillments",
                "discount_id",
                "line_items",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "customer_id",
                "tender_array"
            ).collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected

        }
    }
    test("testTransform") {
        import spark.implicits._
        val tableName = "raw_orders"
        // given
        val dateNow = "2022-07-05"
        val orderId1 = "orderId1"
        val orderId2 = "orderId2"
        val partnerId = "testPartnerId"
        val dtFormatShort = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())

        val dateTimeCreatedAt = "2022-07-05T12:42Z"
        val dateTimeClosedAt = dateTimeCreatedAt // "2022-07-05T12:43Z"

        implicit val formats: DefaultFormats = DefaultFormats
        val inputData = List(
            RawDataEmulator(
                // case for normal flow
                dateNow,
                Serialization.write(
                    new OrderSummaryFromRaw(
                        id = orderId1,
                        created_at = dateTimeCreatedAt,
                        closed_at = dateTimeClosedAt,
                        total_money = Money(amount = 224),
                        fulfillments = "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
                        line_items = "[{'catalog_object_id':1, 'quantity':2}]",
                        total_service_charge_money = Money(703),
                        total_tax_money = Money(10),
                        total_tip_money = Money(5),
                        total_discount_money = Money(1),
                        customer_id = "ABC-123",
                        tenders = "{'id':42}",
                        location_id = "dsdsd",
                        state = "COMPLETED",
                        discounts = Discount(uid = "uidABC")
                    )
                )
            ),
            // case for NULL date check
            RawDataEmulator(
                dateNow,
                Serialization.write(
                    new OrderSummaryFromRaw(
                        id = orderId2,
                        created_at = dateTimeCreatedAt,
                        closed_at = null,
                        total_money = Money(amount = 511),
                        fulfillments = "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
                        line_items = "[{'catalog_object_id':1, 'quantity':2}]",
                        total_service_charge_money = Money(128),
                        total_tax_money = Money(10),
                        total_tip_money = Money(5),
                        total_discount_money = Money(1),
                        customer_id = "XYZ:789",
                        tenders = "{'id':17}",
                        location_id = "dsdsd",
                        state = "COMPLETED",
                        discounts = Discount(uid = "uidXYZ")
                    )
                )
            )
        )

        val rawOrderSummary = inputData.toDF

        val identityIndexes = List(IdentityTestData(orderId1), IdentityTestData(orderId2)).toDF

        // GlobalTempView used since it allows place data
        // to context where SQL works as "SELECT * FROM global_temp.tableName"
        rawOrderSummary.createOrReplaceGlobalTempView(tableName)

        // when
        val orderSummary = OrderSummaryProcessor.readOrderSummary(spark, dateNow, "global_temp", tableName)
        val processedOrderSummary =
            OrderSummaryProcessor.transformOrderSummary(orderSummary, dateNow, partnerId, identityIndexes)

        // then
        val actualFieldsReturned = processedOrderSummary.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + processedOrderSummary.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "ord_id",
                "ord_desc",
                "ord_total",
                "ord_date",
                "ord_timestamp",
                "discount_amount",
                "cxi_partner_id",
                "location_id",
                "ord_state",
                "ord_type",
                "ord_originate_channel_id",
                "ord_target_channel_id",
                "item_quantity",
                "item_total",
                "emp_id",
                "discount_id",
                "dsp_qty",
                "dsp_ttl",
                "guest_check_line_item_id",
                "line_id",
                "taxes_id",
                "taxes_amount",
                "item_id",
                "item_price_id",
                "reason_code_id",
                "service_charge_id",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "tender_ids",
                "ord_pay_total",
                "ord_sub_total",
                "feed_date"
            )
        }
        val actualSquareLocationsData = processedOrderSummary.collect

        val expectedLst = List(
            new OrderSummaryTransformResult(
                ord_id = orderId1,
                ord_desc = null,
                ord_total = BigDecimal.valueOf(2.24),
                ord_date = "2022-07-05T12:42Z",
                ord_timestamp = "2022-07-05T12:42Z",
                discount_amount = BigDecimal.valueOf(0.01),
                cxi_partner_id = "testPartnerId",
                location_id = "dsdsd",
                ord_state = "COMPLETED",
                ord_type = null,
                ord_originate_channel_id = 1,
                ord_target_channel_id = 0,
                item_quantity = "2",
                item_total = null,
                emp_id = null,
                discount_id = "uidABC",
                dsp_qty = null,
                dsp_ttl = null,
                guest_check_line_item_id = null,
                line_id = null,
                taxes_id = null,
                taxes_amount = null,
                item_id = "1",
                item_price_id = null,
                reason_code_id = null,
                service_charge_id = null,
                service_charge_amount = BigDecimal.valueOf(7.03),
                total_taxes_amount = BigDecimal.valueOf(.10),
                total_tip_amount = BigDecimal.valueOf(0.05),
                tender_ids = Seq("42"),
                ord_pay_total = BigDecimal.valueOf(2.24),
                ord_sub_total = BigDecimal.valueOf(-4.94),
                feed_date = java.sql.Date.valueOf(LocalDate.parse(dateNow, dtFormatShort))
            ),
            new OrderSummaryTransformResult(
                ord_id = orderId2,
                ord_desc = null,
                ord_total = BigDecimal.valueOf(5.11),
                ord_date = "2022-07-05T12:42Z",
                ord_timestamp = "2022-07-05T12:42Z",
                discount_amount = BigDecimal.valueOf(0.01),
                cxi_partner_id = "testPartnerId",
                location_id = "dsdsd",
                ord_state = "COMPLETED",
                ord_type = null,
                ord_originate_channel_id = 1,
                ord_target_channel_id = 0,
                item_quantity = "2",
                item_total = null,
                emp_id = null,
                discount_id = "uidXYZ",
                dsp_qty = null,
                dsp_ttl = null,
                guest_check_line_item_id = null,
                line_id = null,
                taxes_id = null,
                taxes_amount = null,
                item_id = "1",
                item_price_id = null,
                reason_code_id = null,
                service_charge_id = null,
                service_charge_amount = BigDecimal.valueOf(1.28),
                total_taxes_amount = BigDecimal.valueOf(0.10),
                total_tip_amount = BigDecimal.valueOf(0.05),
                tender_ids = Seq("17"),
                ord_pay_total = BigDecimal.valueOf(5.11),
                ord_sub_total = BigDecimal.valueOf(3.68),
                feed_date = java.sql.Date.valueOf(LocalDate.parse(dateNow, dtFormatShort))
            )
        )

        var expected = expectedLst.toDF
        // Need ensure issue of Spark  https://issues.apache.org/jira/browse/SPARK-18484
        // doesn't affect field comparison
        expected.schema.fields.foreach { field =>
            if (field.dataType.isInstanceOf[DecimalType]) {
                expected = expected
                    .withColumn(field.name, expected(field.name).cast(DecimalType(9, 2)))
            }
        }

        withClue("read JSON order summary does not match") {
            val collectedExpected = expected.collect
            collectedExpected should contain theSameElementsAs actualSquareLocationsData
        }
    }

}

object OrderSummaryProcessorTest {

    case class OrdTargetChannelIdTestCase(fulfillments: Seq[Fulfillment], expectedOrdTargetChannelId: Int)

    case class Money(amount: Int)

    case class Discount(uid: String)

    case class OrderSummaryFromRaw(
        id: String,
        created_at: String,
        closed_at: String,
        total_money: Money,
        fulfillments: String,
        line_items: String,
        total_service_charge_money: Money,
        total_tax_money: Money,
        total_tip_money: Money,
        total_discount_money: Money,
        customer_id: String,
        tenders: String,
        location_id: String,
        state: String,
        discounts: Discount
    )
    case class OrderSummaryTransformResult(
        ord_id: String,
        ord_desc: String,
        ord_total: BigDecimal,
        ord_date: String,
        ord_timestamp: String,
        discount_amount: BigDecimal,
        cxi_partner_id: String,
        location_id: String,
        ord_state: String,
        ord_type: String,
        ord_originate_channel_id: Int,
        ord_target_channel_id: Int,
        item_quantity: String,
        item_total: BigDecimal,
        emp_id: String,
        discount_id: String,
        dsp_qty: String,
        dsp_ttl: String,
        guest_check_line_item_id: String,
        line_id: String,
        taxes_id: Seq[String],
        taxes_amount: BigDecimal,
        item_id: String,
        item_price_id: String,
        reason_code_id: String,
        service_charge_id: String,
        service_charge_amount: BigDecimal,
        total_taxes_amount: BigDecimal,
        total_tip_amount: BigDecimal,
        tender_ids: Seq[String],
        ord_pay_total: BigDecimal,
        ord_sub_total: BigDecimal,
        feed_date: java.sql.Date
    )

    case class RawDataEmulator(feed_date: String, record_value: String, record_type: String = "orders")

    case class IdentityTestData(ord_id: String)
}

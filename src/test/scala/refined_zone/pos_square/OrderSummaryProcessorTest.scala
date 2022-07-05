package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, PickupDetails}
import refined_zone.hub.model.{ChannelType, OrderStateType}
import support.normalization.DateNormalization.parseToSqlDate
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._
    import OrderSummaryProcessorTest._

    test("getOrdTargetChannelId") {
        import Fulfillment.{State, Type}

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

    test("transform order summary") {

        // given
        val line_items =
            """
               [
                 {
                   "applied_taxes": [
                     {
                       "applied_money": {
                         "amount": 66,
                         "currency": "USD"
                       },
                       "tax_uid": "tax_uid_1",
                       "uid": "uid_1"
                     }
                   ],
                   "base_price_money": {
                     "amount": 945,
                     "currency": "USD"
                   },
                   "catalog_object_id": "obj_1",
                   "catalog_version": 123,
                   "gross_sales_money": {
                     "amount": 1095,
                     "currency": "USD"
                   },
                   "item_type": "ITEM",
                   "modifiers": [
                     {
                       "base_price_money": {
                         "amount": 150,
                         "currency": "USD"
                       },
                       "catalog_object_id": "obj_1",
                       "catalog_version": 123,
                       "name": "Extra Meat",
                       "total_price_money": {
                         "amount": 150,
                         "currency": "USD"
                       },
                       "uid": "uid_1"
                     }
                   ],
                   "name": "Combo Bop",
                   "note": "Ext sloppy",
                   "quantity": "1",
                   "total_discount_money": {
                     "amount": 0,
                     "currency": "USD"
                   },
                   "total_money": {
                     "amount": 1161,
                     "currency": "USD"
                   },
                   "total_tax_money": {
                     "amount": 66,
                     "currency": "USD"
                   },
                   "uid": "uid_1",
                   "variation_name": "Regular",
                   "variation_total_price_money": {
                     "amount": 945,
                     "currency": "USD"
                   }
                 }
               ]
              """

        val tenders =
            """
               [
                 {
                   "amount_money": {
                     "amount": 1161,
                     "currency": "USD"
                   },
                   "card_details": {
                     "card": {
                       "card_brand": "MASTERCARD",
                       "fingerprint": "abc123",
                       "last_4": "0000"
                     },
                     "entry_method": "EMV",
                     "status": "CAPTURED"
                   },
                   "created_at": "2021-12-28T02:24:56Z",
                   "customer_id": "cust_1",
                   "id": "t_01",
                   "location_id": "loc_1",
                   "processing_fee_money": {
                     "amount": 34,
                     "currency": "USD"
                   },
                   "transaction_id": "txn_1",
                   "type": "CARD"
                 }
               ]
              """.stripMargin

        val orderSummary = Seq(
            (
                "ord_1",
                "100",
                "5",
                "2021-12-28",
                "2021-12-28T02:25:13Z",
                "loc_1",
                "COMPLETED",
                null,
                null,
                line_items,
                "0",
                "10",
                "1",
                null,
                tenders
            )
        ).toDF(
            "ord_id",
            "ord_total",
            "discount_amount",
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

        val cxiIdentityIdsByOrder = Seq(
            ("ord_1", Array(Map("combination-bin" -> "comb-bin-123")))
        ).toDF("ord_id", "cxi_identity_ids")

        // when
        val actual =
            OrderSummaryProcessor.transformOrderSummary(orderSummary, "2022-02-24", "partner-1", cxiIdentityIdsByOrder)

        // then
        val expectedData = Seq(
            Row(
                "ord_1",
                null,
                BigDecimal(1.0),
                "2021-12-28",
                "2021-12-28T02:25:13Z",
                BigDecimal(0.05),
                "partner-1",
                "loc_1",
                OrderStateType.Completed.code,
                null,
                1,
                0,
                "1",
                BigDecimal(11.61),
                null,
                null,
                null,
                null,
                "uid_1",
                null,
                Array("tax_uid_1"),
                BigDecimal(0.66),
                "obj_1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.10),
                BigDecimal(0.01),
                Array("t_01"),
                BigDecimal(1.00),
                BigDecimal(0.89),
                parseToSqlDate("2022-02-24"),
                Array(Map("combination-bin" -> "comb-bin-123"))
            )
        )
        val orderSummarySchema = DataTypes.createStructType(
            Array(
                StructField("ord_id", StringType),
                StructField("ord_desc", NullType),
                StructField("ord_total", DecimalType(9, 2)),
                StructField("ord_date", StringType),
                StructField("ord_timestamp", StringType),
                StructField("discount_amount", DecimalType(9, 2)),
                StructField("cxi_partner_id", StringType, nullable = false),
                StructField("location_id", StringType),
                StructField("ord_state_id", IntegerType, nullable = false),
                StructField("ord_type", NullType),
                StructField("ord_originate_channel_id", IntegerType, nullable = false),
                StructField("ord_target_channel_id", IntegerType, nullable = false),
                StructField("item_quantity", StringType),
                StructField("item_total", DecimalType(9, 2)),
                StructField("emp_id", NullType),
                StructField("discount_id", NullType),
                StructField("dsp_qty", NullType),
                StructField("dsp_ttl", NullType),
                StructField("guest_check_line_item_id", StringType),
                StructField("line_id", NullType),
                StructField("taxes_id", DataTypes.createArrayType(StringType)),
                StructField("taxes_amount", DecimalType(9, 2)),
                StructField("item_id", StringType),
                StructField("item_price_id", NullType),
                StructField("reason_code_id", NullType),
                StructField("service_charge_id", NullType),
                StructField("service_charge_amount", DecimalType(9, 2)),
                StructField("total_taxes_amount", DecimalType(9, 2)),
                StructField("total_tip_amount", DecimalType(9, 2)),
                StructField("tender_ids", DataTypes.createArrayType(StringType)),
                StructField("ord_pay_total", DecimalType(9, 2)),
                StructField("ord_sub_total", DecimalType(12, 2)),
                StructField("feed_date", DateType),
                StructField(
                    "cxi_identity_ids",
                    DataTypes.createArrayType(DataTypes.createMapType(StringType, StringType))
                )
            )
        )
        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedData.asJava, orderSummarySchema)
        withClue("order summary is not correctly transformed") {
            assertDataFrameEquals(expected, actual)
        }
    }

}

object OrderSummaryProcessorTest {
    case class OrdTargetChannelIdTestCase(fulfillments: Seq[Fulfillment], expectedOrdTargetChannelId: Int)
}

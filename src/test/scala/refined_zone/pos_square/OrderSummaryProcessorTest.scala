package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, PickupDetails}
import refined_zone.hub.model.{ChannelType, OrderStateType}
import support.normalization.DateNormalization
import support.normalization.DateNormalization.parseToSqlDate
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.scalatest.Matchers

import java.time.format.DateTimeFormatter
import java.time.ZoneId

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._
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
        // given
        val tableName = "raw_orders"
        val dateNow = "2022-07-05"
        val rawOrderSummary = List(
            (
                dateNow,
                """
                  {
                    "id": "orderId1",
                    "created_at": "2022-07-05T12:42Z",
                    "closed_at": "2022-07-05T12:42Z",
                    "total_money": {
                        "amount": 224
                    },
                    "fulfillments": "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
                    "line_items": "[{'catalog_object_id':1, 'quantity':2}]",
                    "total_service_charge_money": {
                        "amount": 703
                    },
                    "total_tax_money": {
                        "amount": 10
                    },
                    "total_tip_money": {
                        "amount": 5
                    },
                    "total_discount_money": {
                        "amount": 1
                    },
                    "customer_id": "ABC-123",
                    "tenders": "{'id':42}",
                    "location_id": "dsdsd",
                    "state": "COMPLETED",
                    "discounts": {
                        "uid": "uidABC"
                    }
                }
                """.stripMargin,
                "orders"
            ),
            (
                dateNow,
                """
                  {
                    "id": "orderId2",
                    "created_at": "2022-07-05T12:42Z",
                    "closed_at": null,
                    "total_money": {
                        "amount": 45
                    },
                    "fulfillments": "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
                    "line_items": "[{'catalog_object_id':1, 'quantity':2}]",
                    "total_service_charge_money": {
                        "amount": 746
                    },
                    "total_tax_money": {
                        "amount": 10
                    },
                    "total_tip_money": {
                        "amount": 5
                    },
                    "total_discount_money": {
                        "amount": 1
                    },
                    "customer_id": "XYZ:789",
                    "tenders": "{'id':17}",
                    "location_id": "dsdsd",
                    "state": "COMPLETED",
                    "discounts": {
                        "uid": "uidXYZ"
                    }
                }
                """.stripMargin,
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
    test("testTransform With Close Date missed") {
        import spark.implicits._
        // given
        val tableName = "raw_orders"
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
                    OrderSummaryFromRaw(
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
                    OrderSummaryFromRaw(
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
                "ord_state_id",
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
            OrderSummaryTransformResult(
                ord_id = orderId1,
                ord_desc = null,
                ord_total = BigDecimal.valueOf(2.24),
                ord_date = "2022-07-05T12:42Z",
                ord_timestamp = "2022-07-05T12:42Z",
                discount_amount = BigDecimal.valueOf(0.01),
                cxi_partner_id = "testPartnerId",
                location_id = "dsdsd",
                ord_state_id = OrderStateType.Completed.code,
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
                feed_date = DateNormalization.parseToSqlDate(dateNow).get
            ),
            OrderSummaryTransformResult(
                ord_id = orderId2,
                ord_desc = null,
                ord_total = BigDecimal.valueOf(5.11),
                ord_date = "2022-07-05T12:42Z",
                ord_timestamp = "2022-07-05T12:42Z",
                discount_amount = BigDecimal.valueOf(0.01),
                cxi_partner_id = "testPartnerId",
                location_id = "dsdsd",
                ord_state_id = OrderStateType.Completed.code,
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
                feed_date = DateNormalization.parseToSqlDate(dateNow).get
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
            "created_at",
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
        ord_state_id: Int,
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

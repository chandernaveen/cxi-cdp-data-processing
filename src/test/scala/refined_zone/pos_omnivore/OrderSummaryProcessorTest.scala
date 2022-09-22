package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.OrderType
import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import support.normalization.DateNormalization.parseToSqlDate
import support.normalization.TimestampNormalization.parseToTimestamp
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._

    test("Reading order summary from raw") {

        // given
        val tableName = "raw_tickets"
        val feedDate = "2022-07-28"
        val rawTickets = List(
            (
                "tickets",
                """
                {
                    "_embedded": {
                        "employee": {
                            "id": "empId_1"
                        },
                        "order_type": {
                            "name": "Eat In"
                        },
                        "items": [],
                        "service_charges": [],
                        "payments": [{
                            "amount": 5240,
                            "change": 0,
                            "id": "20",
                            "last4": null,
                            "status": null,
                            "tip": 0,
                            "type": "cash"
                        }]
                    },
                    "_links": {
                        "self": {
                            "href": "https://api.omnivore.io/1.0/locations/cRyzGpdi/tickets/14/",
                            "type": "application/hal+json; name=ticket"
                        }
                    },
                    "totals": {
                      "discounts": 0,
                      "service_charges": 0,
                      "tax": 714,
                      "tips": 0,
                      "paid": 0,
                      "sub_total": 9100,
                      "total": 9814
                    },
                    "closed_at": 1657213110,
                    "id": "ordId_1",
                    "open": false,
                    "opened_at": 1657212092,
                    "void": false
                }
                """.stripMargin,
                feedDate,
                "file_name1",
                "cxi_id1"
            )
        ).toDF("record_type", "record_value", "feed_date", "file_name", "cxi_id")

        // GlobalTempView used since it allows place data
        // to context where SQL works as "SELECT * FROM global_temp.tableName"
        rawTickets.createOrReplaceGlobalTempView(tableName)

        // when
        val ticketsActual = OrderSummaryProcessor.readOrderSummary(spark, feedDate, "global_temp", tableName)

        // then
        val actualFieldsReturned = ticketsActual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + ticketsActual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "ord_id",
                "ord_total",
                "opened_at",
                "closed_at",
                "location_url",
                "open",
                "void",
                "ord_type",
                "emp_id",
                "discount_amount",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "ord_sub_total",
                "ord_pay_total",
                "items",
                "service_charges",
                "payments"
            )
        }

        val actualOrderCustomerData = ticketsActual.collect
        withClue("Read order customer data should match") {
            val expected = List(
                (
                    "ordId_1",
                    "9814",
                    "1657212092",
                    "1657213110",
                    "https://api.omnivore.io/1.0/locations/cRyzGpdi/tickets/14/",
                    "false",
                    "false",
                    """{"name":"Eat In"}""",
                    "empId_1",
                    "0",
                    "0",
                    "714",
                    "0",
                    "9100",
                    "0",
                    "[]",
                    "[]",
                    """
                    [{
                        "amount": 5240,
                        "change": 0,
                        "id": "20",
                        "last4": null,
                        "status": null,
                        "tip": 0,
                        "type": "cash"
                    }]
                    """.replaceAll("\\s", "")
                )
            ).toDF(
                "ord_id",
                "ord_total",
                "opened_at",
                "closed_at",
                "location_url",
                "open",
                "void",
                "ord_type",
                "emp_id",
                "discount_amount",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "ord_sub_total",
                "ord_pay_total",
                "items",
                "service_charges",
                "payments"
            ).collect()

            actualOrderCustomerData.length should equal(expected.length)
            actualOrderCustomerData should contain theSameElementsAs expected
        }
    }

    test("Transform order summary") {

        // given
        val orderType_1 =
            """
            {
                "available": true,
                "id": "1",
                "name": "Dine In",
                "pos_id": "1"
            }
            """.stripMargin

        val orderType_2 =
            """
            {
                "name": "Delivery"
            }
            """.stripMargin

        val items =
            """
               [{
                "_embedded": {
                  "menu_item": {
                    "_embedded": {
                      "menu_categories": [{
                        "id": "f1000",
                        "level": 0,
                        "name": "APPETIZERS",
                        "pos_id": "1000"
                      }],
                      "price_levels": [{
                        "barcodes": [],
                        "id": "1",
                        "name": "REG",
                        "price_per_unit": 199
                      }]
                    },
                    "barcodes": [],
                    "id": "1001",
                    "in_stock": true,
                    "name": "Coffee",
                    "open": false,
                    "open_name": false,
                    "pos_id": "1001",
                    "price_per_unit": 199
                  }
                },
                "id": "63323-1",
                "included_tax": 0,
                "name": "Coffee",
                "price": 199,
                "quantity": 1,
                "seat": 1,
                "sent": true,
                "sent_at": 1656519489,
                "split": 1
              }]
              """.stripMargin

        val payments =
            """
              [{
                "_embedded": {
                  "tender_type": {
                    "allows_tips": true,
                    "id": "123",
                    "name": "3rd Party",
                    "pos_id": "107"
                  }
                },
                "amount": 199,
                "id": "26",
                "tip": 40,
                "type": "other"
              }]
              """.stripMargin

        val orderSummary = Seq(
            (
                "ord_1",
                215,
                1657212092,
                1657213110,
                "https://api.omnivore.io/1.0/locations/loc-id1/tickets/14782/",
                false,
                false,
                orderType_1,
                "123",
                0,
                0,
                16,
                40,
                199,
                215,
                items,
                null,
                payments
            ),
            (
                "ord_2",
                215,
                1657212092,
                1657213110,
                "https://api.omnivore.io/1.0/locations/loc-id2/tickets/14782/",
                false,
                false,
                orderType_2,
                "123",
                0,
                0,
                16,
                40,
                199,
                215,
                items,
                null,
                payments
            )
        ).toDF(
            "ord_id",
            "ord_total",
            "opened_at",
            "closed_at",
            "location_url",
            "open",
            "void",
            "ord_type",
            "emp_id",
            "discount_amount",
            "service_charge_amount",
            "total_taxes_amount",
            "total_tip_amount",
            "ord_sub_total",
            "ord_pay_total",
            "items",
            "service_charges",
            "payments"
        )

        val posIdentityIdsByOrder = Seq(
            (
                "ord_1",
                "loc-id1",
                Array(Map("identity_type" -> "combination-card-slim", "identity_id" -> "card-slim-id"))
            )
        ).toDF("ord_id", "location_id", "cxi_identity_ids")

        // when
        val actual =
            OrderSummaryProcessor.transformOrderSummary(
                orderSummary,
                "2022-07-09",
                "partner_1",
                posIdentityIdsByOrder
            )

        // then
        val expectedData = Seq(
            Row(
                "ord_1",
                "loc-id1",
                null,
                BigDecimal(2.15),
                parseToSqlDate("2022-07-07"),
                parseToTimestamp("2022-07-07T16:58:30Z"),
                "partner_1",
                OrderStateType.Completed.code,
                null,
                OrderChannelType.PhysicalLane.code,
                OrderChannelType.PhysicalLane.code,
                1,
                BigDecimal(1.99),
                "123",
                null,
                BigDecimal(0.00),
                null,
                null,
                null,
                null,
                null,
                BigDecimal(0.00),
                "63323-1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.16),
                BigDecimal(0.40),
                Array("123"),
                BigDecimal(1.99),
                BigDecimal(2.15),
                parseToSqlDate("2022-07-09"),
                Array(Map("identity_type" -> "combination-card-slim", "identity_id" -> "card-slim-id"))
            ),
            Row(
                "ord_2",
                "loc-id2",
                null,
                BigDecimal(2.15),
                parseToSqlDate("2022-07-07"),
                parseToTimestamp("2022-07-07T16:58:30Z"),
                "partner_1",
                OrderStateType.Completed.code,
                null,
                OrderChannelType.Other.code,
                OrderChannelType.PhysicalDelivery.code,
                1,
                BigDecimal(1.99),
                "123",
                null,
                BigDecimal(0.00),
                null,
                null,
                null,
                null,
                null,
                BigDecimal(0.00),
                "63323-1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.16),
                BigDecimal(0.40),
                Array("123"),
                BigDecimal(1.99),
                BigDecimal(2.15),
                parseToSqlDate("2022-07-09"),
                null
            )
        )

        val orderSummarySchema = DataTypes.createStructType(
            Array(
                StructField("ord_id", StringType),
                StructField("location_id", StringType),
                StructField("ord_desc", NullType),
                StructField("ord_total", DecimalType(9, 2)),
                StructField("ord_date", DateType, nullable = false),
                StructField("ord_timestamp", TimestampType, nullable = false),
                StructField("cxi_partner_id", StringType, nullable = false),
                StructField("ord_state_id", IntegerType, nullable = false),
                StructField("ord_type", NullType),
                StructField("ord_originate_channel_id", IntegerType),
                StructField("ord_target_channel_id", IntegerType),
                StructField("item_quantity", IntegerType),
                StructField("item_total", DecimalType(9, 2)),
                StructField("emp_id", StringType),
                StructField("discount_id", NullType),
                StructField("discount_amount", DecimalType(9, 2)),
                StructField("dsp_qty", NullType),
                StructField("dsp_ttl", NullType),
                StructField("guest_check_line_item_id", NullType),
                StructField("line_id", NullType),
                StructField("taxes_id", NullType),
                StructField("taxes_amount", DecimalType(9, 2)),
                StructField("item_id", StringType),
                StructField("item_price_id", NullType),
                StructField("reason_code_id", NullType),
                StructField("service_charge_id", NullType),
                StructField("service_charge_amount", DecimalType(9, 2)),
                StructField("total_taxes_amount", DecimalType(9, 2)),
                StructField("total_tip_amount", DecimalType(9, 2)),
                StructField("tender_ids", DataTypes.createArrayType(StringType)),
                StructField("ord_sub_total", DecimalType(9, 2)),
                StructField("ord_pay_total", DecimalType(9, 2)),
                StructField("feed_date", DateType),
                StructField(
                    "cxi_identity_ids",
                    DataTypes.createArrayType(DataTypes.createMapType(StringType, StringType))
                )
            )
        )

        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedData.asJava, orderSummarySchema)

        withClue("Order Summary data do not match") {
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("normalizeOrderOriginateChannelTypeUdf") {
        // given
        val testCaseDF = Seq(
            (null, OrderChannelType.Unknown.code),
            (OrderType(name = null), OrderChannelType.Unknown.code),
            (OrderType(name = ""), OrderChannelType.Unknown.code),
            (OrderType(name = "Dine In"), OrderChannelType.PhysicalLane.code),
            (OrderType(name = "Eat In"), OrderChannelType.PhysicalLane.code),
            (OrderType(name = "Carry Out"), OrderChannelType.PhysicalLane.code),
            (OrderType(name = "Delivery"), OrderChannelType.Other.code),
            (OrderType(name = "Order Ahead"), OrderChannelType.Other.code)
        ).toDF("ord_type", "expected_ord_originate_channel_id")

        val inputDF = testCaseDF.select("ord_type")

        // when
        val actual = inputDF
            .withColumn(
                "ord_originate_channel_id",
                OrderSummaryProcessor.normalizeOrderOriginateChannelTypeUdf(col("ord_type"))
            )

        // then
        val expected = testCaseDF.withColumnRenamed("expected_ord_originate_channel_id", "ord_originate_channel_id")

        expected.schema.fieldNames shouldBe actual.schema.fieldNames
        assertDataFrameDataEquals(expected, actual)
    }

    test("normalizeOrderTargethannelTypeUdf") {
        // given
        val testCaseDF = Seq(
            (null, OrderChannelType.Unknown.code),
            (OrderType(name = null), OrderChannelType.Unknown.code),
            (OrderType(name = ""), OrderChannelType.Unknown.code),
            (OrderType(name = "Dine In"), OrderChannelType.PhysicalLane.code),
            (OrderType(name = "Eat In"), OrderChannelType.PhysicalLane.code),
            (OrderType(name = "Carry Out"), OrderChannelType.PhysicalPickup.code),
            (OrderType(name = "Delivery"), OrderChannelType.PhysicalDelivery.code),
            (OrderType(name = "Order Ahead"), OrderChannelType.PhysicalLane.code)
        ).toDF("ord_type", "expected_ord_target_channel_id")

        val inputDF = testCaseDF.select("ord_type")

        // when
        val actual = inputDF
            .withColumn(
                "ord_target_channel_id",
                OrderSummaryProcessor.normalizeOrderTargetChannelTypeUdf(col("ord_type"))
            )

        // then
        val expected = testCaseDF.withColumnRenamed("expected_ord_target_channel_id", "ord_target_channel_id")

        expected.schema.fieldNames shouldBe actual.schema.fieldNames
        assertDataFrameDataEquals(expected, actual)
    }
}

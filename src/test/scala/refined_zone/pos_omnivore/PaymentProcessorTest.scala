package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers

class PaymentProcessorTest extends BaseSparkBatchJobTest with Matchers {

    test("Read Payment Processing") {
        import spark.implicits._

        val TicketFile =
            List(
                (
                    s"""
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
                            "payments": [ {
                            "id": "123"
                         }]
                        }
                        ,
                        "_links": {
                            "self": {
                            "href": "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14774/",
                            "type": "application/hal+json; name=ticket"
                        }
                        }
                        ,
                        "totals": {
                            "discounts": 0,
                            "service_charges": 0,
                            "tax": 714,
                            "tips": 0,
                            "paid": 0,
                            "sub_total": 9100,
                            "total": 9814
                        }
                        ,
                        "closed_at": 1657340725
                        ,
                        "id": "14774"
                        ,
                        "open": false
                        ,
                        "opened_at": 1657212092
                        ,
                        "void": false
                    }
        """.stripMargin,
                    "tickets",
                    "2022-04-13"
                )
            ).toDF("record_value", "record_type", "feed_date")

        val tableName = "TicketsSummary"
        TicketFile.createOrReplaceTempView(tableName)

        // when
        val actualRead = PaymentsProcessor.readPayments(spark, "2022-04-13", tableName)

        // then
        val actualFieldsReturned = actualRead.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualRead.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "payments",
                "order_id",
                "location_url"
            )

        }

        val paymentsReadExpected = List(
            (
                s"""[{"id":"123"}]""",
                "14774", // display_name
                "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14774/"
            )
        ).toDF(
            "payments",
            "order_id",
            "location_url"
        ).collect()

        val actualOmnivorePaymentReadData = actualRead.collect()

        withClue("POS Omnivore raw- payments data do not match") {
            actualOmnivorePaymentReadData.length should equal(paymentsReadExpected.length)
            actualOmnivorePaymentReadData should contain theSameElementsAs paymentsReadExpected
        }

    }

    test("transform Payment Processing") {
        import spark.implicits._
        // given
        val items =
            """
               [{
                "_embedded": {
                  "modifiers":[
                    {"id":"Variation-1"},{"id":"Variation-2"}
                   ],
                  "menu_item": {
                    "_embedded": {
                      "menu_categories": [{
                        "id": "f1000",
                        "level": 0,
                        "name": "APPETIZERS",
                        "pos_id": "1000"
                      },{
                        "id": "f2000",
                        "level": 0,
                        "name": "APPETIZERS",
                        "pos_id": "1000"
                      }],
                      "price_levels": [{

                        "barcodes": [{"id":"Bar1"},{"id":"Bar-2"}],
                        "id": "1",
                        "name": "REG",
                        "price_per_unit": 199
                      }]
                    },
                    "barcodes": [{"id":123},{"id":346}],
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

        val TicketsSummary = Seq(
            (
                "ord_1",
                215,
                1656519489,
                1656519489,
                "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14782/",
                false,
                false,
                "Eat In",
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
            "order_id",
            "ord_total",
            "opened_at",
            "closed_at",
            "location_url",
            "open",
            "void",
            "order_type",
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

        val actual =
            PaymentsProcessor.transformPayments(TicketsSummary, "partner-1")

        // then
        val expectedData = Seq(
            Row("partner-1", "26", null, "other", "cjgjjkpi", "ord_1", null, null, null, null, null)
        )

        val PaymentsSchema = DataTypes.createStructType(
            Array(
                StructField("cxi_partner_id", StringType, false),
                StructField("payment_id", StringType),
                StructField("status", NullType),
                StructField("name", StringType),
                StructField("location_id", StringType),
                StructField("order_id", StringType),
                StructField("card_brand", NullType),
                StructField("pan", NullType),
                StructField("bin", NullType),
                StructField("exp_month", NullType),
                StructField("exp_year", NullType)
            )
        )

        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedData.asJava, PaymentsSchema)

        withClue("Payment processor data were not correctly transformed") {

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}

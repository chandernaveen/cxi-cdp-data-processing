package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers

class OrderTendersProcessorTest extends BaseSparkBatchJobTest with Matchers {

    test("Read Order Tender Processing") {
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
        val actualRead = OrderTendersProcessor.readOrderTenders(spark, "2022-04-13", tableName)

        // then
        val actualFieldsReturned = actualRead.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualRead.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "payments",
                "location_url"
            )

        }

        val OrderTenderReadExpected = List(
            (
                "[{\"id\":\"123\"}]", // id
                "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14774/"
            )
        ).toDF(
            "payments",
            "location_url"
        ).collect()

        val actualOmnivoreOrderTenderReadData = actualRead.collect()

        withClue("POS Omnivore raw- OrderTender data do not match") {
            actualOmnivoreOrderTenderReadData.length should equal(OrderTenderReadExpected.length)
            actualOmnivoreOrderTenderReadData should contain theSameElementsAs OrderTenderReadExpected
        }

    }

    test("transform OrderTender Processing") {
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

        val TicketSummary = Seq(
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
            OrderTendersProcessor.transformOrderTenders(TicketSummary, "partner-1")

        // then
        val expectedData = Seq(
            Row(
                "123",
                "partner-1",
                "cjgjjkpi",
                "3rd Party",
                99
            )
        )

        val orderTenderSchema = DataTypes.createStructType(
            Array(
                StructField("tender_id", StringType),
                StructField("cxi_partner_id", StringType),
                StructField("location_id", StringType),
                StructField("tender_nm", StringType),
                StructField("tender_type", IntegerType)
            )
        )

        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedData.asJava, orderTenderSchema)

        withClue("Order Tender data were not correctly transformed") {

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()

        }
    }

}

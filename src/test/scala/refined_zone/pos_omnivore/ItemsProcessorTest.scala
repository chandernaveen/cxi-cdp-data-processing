package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers

class ItemsProcessorTest extends BaseSparkBatchJobTest with Matchers {

    test("Read MenuItem Processing") {
        import spark.implicits._

        val TicketFile =
            List(
                (
                    s"""
                   |{
                   |  "_embedded": {
                   |
                   |    "items": [
                   |
                   |       { "id": "63308-3",
                   |        "name": "THE MARGHERITA"
                   |
                   |      }
                   |    ]
                   |     },
                   |  "_links": {
                   |    "self": {
                   |      "href": "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14774/",
                   |      "type": "application/hal+json; name=ticket"
                   |    }
                   |    },
                   |  "id": "14774",
                   |  "name": "API33b431ece68c",
                   |  "ticket_number": 1290
                   |
                   |}
                   |""".stripMargin,
                    "tickets",
                    "2022-04-13"
                )
            ).toDF("record_value", "record_type", "feed_date")

        val tableName = "TicketsSummary"
        TicketFile.createOrReplaceTempView(tableName)

        // when
        val actualRead = ItemsProcessor.readItems(spark, "2022-04-13", tableName)

        // then
        val actualFieldsReturned = actualRead.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualRead.schema.treeString) {
            actualFieldsReturned shouldEqual Array("items")

        }

        val ItemsReadExpected = List(
            (
                s"""[{"id":"63308-3","name":"THE MARGHERITA"}]"""
            )
        ).toDF(
            "items"
        ).collect()

        val actualOmnivoreItemReadData = actualRead.collect()

        withClue("POS Omnivore raw- MenuItem data do not match") {
            actualOmnivoreItemReadData.length should equal(ItemsReadExpected.length)
            actualOmnivoreItemReadData should contain theSameElementsAs ItemsReadExpected
        }

    }

    test("transform Item (Menu Item)") {

        import spark.implicits._

        // given
        val items =
            """
               [{
                "_embedded": {
                  "modifiers":[{"id":"Variation-1"},{"id":"Variation-2"}

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
                        "name": "FOOD",
                        "pos_id": "1000"
                      }],
                      "price_levels": [{

                        "barcodes": [{"id":"Bar1"},{"id":"Bar-2"}],
                        "id": "1",
                        "name": "REG",
                        "price_per_unit": 199
                      }]
                    },
                    "barcodes": ["a","b"],
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

        val actual =
            ItemsProcessor.transformItems(TicketsSummary, "partner-1")

        val expectedData = Seq(
            Row(
                "partner-1",
                "63323-1",
                "Coffee",
                "APPETIZERS,FOOD",
                null,
                "VARIATION",
                "FOOD",
                null,
                "a,b",
                "Variation-1,Variation-2"
            )
        )

        val ItemSummarySchema = DataTypes.createStructType(
            Array(
                StructField("cxi_partner_id", StringType, false),
                StructField("item_id", StringType),
                StructField("item_nm", StringType),
                StructField("category_array", StringType, false),
                StructField("item_desc", NullType),
                StructField("item_type", StringType, false),
                StructField("main_category_name", StringType),
                StructField("item_plu", NullType),
                StructField("item_barcode", StringType),
                StructField("variation_array", StringType, false)
            )
        )

        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedData.asJava, ItemSummarySchema)

        withClue("Item (Menu Item) data were not correctly transformed") {

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()

        }
    }

}

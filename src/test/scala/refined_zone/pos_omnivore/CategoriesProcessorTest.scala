package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers

class CategoriesProcessorTest extends BaseSparkBatchJobTest with Matchers {

    test("Read Categories Processing") {
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
        val actualRead = CategoriesProcessor.readCategories(spark, "2022-04-13", tableName)

        // then
        val actualFieldsReturned = actualRead.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualRead.schema.treeString) {
            actualFieldsReturned shouldEqual Array("items", "location_url")

        }

        val CategoriesReadExpected = List(
            (
                s"""[{"id":"63308-3","name":"THE MARGHERITA"}]""",
                "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14774/"
            )
        ).toDF(
            "items",
            "location_url"
        ).collect()

        val actualOmnivoreCategoryReadData = actualRead.collect()

        withClue("POS Omnivore raw- Category data do not match") {
            actualOmnivoreCategoryReadData.length should equal(CategoriesReadExpected.length)
            actualOmnivoreCategoryReadData should contain theSameElementsAs CategoriesReadExpected
        }

    }

    test("transform Categories Processing") {
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
                items,
                "215",
                "1656519489",
                null,
                "false",
                "Eat In",
                "https://api.omnivore.io/1.0/locations/cjgjjkpi/tickets/14782/"
            )
        ).toDF(
            "items",
            "menu_categories",
            "cxi_partner_id",
            "cat_desc",
            "cat_id",
            "cat_nm",
            "location_url"
        )

        // when
        val actual = CategoriesProcessor.transformCategories(TicketSummary, "partner-1")

        val expectedData = Seq(
            Row(
                "partner-1",
                null,
                "f1000",
                "APPETIZERS",
                "cjgjjkpi"
            ),
            Row(
                "partner-1",
                null,
                "f2000",
                "APPETIZERS",
                "cjgjjkpi"
            )
        )
        val CategoriesSchema = DataTypes.createStructType(
            Array(
                StructField("cxi_partner_id", StringType, false),
                StructField("cat_desc", NullType),
                StructField("cat_id", StringType),
                StructField("cat_nm", StringType),
                StructField("location_id", StringType)
            )
        )

        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedData.asJava, CategoriesSchema)

        withClue("Categories data were not correctly transformed") {

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()

        }
    }

}

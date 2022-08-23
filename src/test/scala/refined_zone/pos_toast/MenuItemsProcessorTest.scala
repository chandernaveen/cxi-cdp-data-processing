package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class MenuItemsProcessorTest extends BaseSparkBatchJobTest {

    test("test transformMenuItems") {
        // given
        import spark.implicits._

        val cxiPartnerId = "cxi-usa-some-partner"
        val menuItems = List(
            (
                "9ea8c952-1b18-431e-b179-96159244a3ec",
                "Big Bacon Bowl",
                "516329006",
                Array("34aa73d3-996b-43ff-87aa-b7283e4e417f", "2f99aa77-869c-4267-aa99-179fc41e4562")
            ),
            ("9edf1418-b580-430c-a733-14a5bcaa22ba", "Maple Syrup", "659648980", Array.empty[String])
        ).toDF("item_id", "item_nm", "item_plu", "variation_array")

        val menuGroups = List(
            (
                Array(
                    "04868e23-3e24-474d-b660-67d5043b8b27",
                    "005b8293-c9c8-4ed6-8c97-1b2564e52836",
                    "4bcba811-d0b1-43ad-9cfc-a60a87a06fa6",
                    "46ecd621-1b87-458f-84e2-b3b54569eea7",
                    "adc3c630-4bdc-430a-ae6e-9e47ccda0876",
                    "34aa73d3-996b-43ff-87aa-b7283e4e417f"
                ),
                Array("0f276491-4610-473c-9753-03079ad164e9", "2f01e1ac-2c7d-4262-9bb5-df1cd130e155"),
                Array.empty[String],
                "Scrambles"
            ),
            (Array.empty[String], Array.empty[String], Array("86eab1e4-8541-40f5-87f8-42d839c6b58a"), "Bowl")
        ).toDF("variation_array", "category_array", "items_ids", "main_category_name")
        val menuOptionGroups = List(
            ("86c0f378-e50f-475f-87d0-fbbe9118c40a", "NO"),
            ("86eab1e4-8541-40f5-87f8-42d839c6b58a", "Sauce Choice"),
            ("8764bd57-5eab-49bd-be8b-1d0906fceb74", "Please Remove")
        ).toDF("item_id", "item_nm")

        // when
        val actual = MenuItemsProcessor.transformMenuItems(menuItems, menuGroups, menuOptionGroups, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "item_id",
                "item_nm",
                "item_desc",
                "item_type",
                "category_array",
                "main_category_name",
                "variation_array",
                "item_plu",
                "item_barcode",
                "cxi_partner_id"
            )
        }
        val actualToastData = actual.collect()
        withClue("POS Toast refined data do not match") {
            val expected =
                List(
                    (
                        "86c0f378-e50f-475f-87d0-fbbe9118c40a",
                        "NO",
                        null,
                        "variation",
                        null,
                        null,
                        null,
                        null,
                        null,
                        "cxi-usa-some-partner"
                    ),
                    (
                        "86eab1e4-8541-40f5-87f8-42d839c6b58a",
                        "Sauce Choice",
                        null,
                        "variation",
                        Array.empty[String],
                        "Bowl",
                        Array.empty[String],
                        null,
                        null,
                        "cxi-usa-some-partner"
                    ),
                    (
                        "8764bd57-5eab-49bd-be8b-1d0906fceb74",
                        "Please Remove",
                        null,
                        "variation",
                        null,
                        null,
                        null,
                        null,
                        null,
                        "cxi-usa-some-partner"
                    ),
                    (
                        "9ea8c952-1b18-431e-b179-96159244a3ec",
                        "Big Bacon Bowl",
                        null,
                        "food",
                        null,
                        null,
                        Array("34aa73d3-996b-43ff-87aa-b7283e4e417f", "2f99aa77-869c-4267-aa99-179fc41e4562"),
                        "516329006",
                        null,
                        "cxi-usa-some-partner"
                    ),
                    (
                        "9edf1418-b580-430c-a733-14a5bcaa22ba",
                        "Maple Syrup",
                        null,
                        "food",
                        null,
                        null,
                        Array.empty[String],
                        "659648980",
                        null,
                        "cxi-usa-some-partner"
                    )
                ).toDF(
                    "item_id",
                    "item_nm",
                    "item_desc",
                    "item_type",
                    "category_array",
                    "main_category_name",
                    "variation_array",
                    "item_plu",
                    "item_barcode",
                    "cxi_partner_id"
                ).collect()
            actualToastData should contain theSameElementsAs expected
        }
    }

    test("test readMenuItems") {
        // given
        import spark.implicits._
        val df = List(
            (
                s"""
                    [
                      {
                        "guid": "9ea8c952-1b18-431e-b179-96159244a3ec",
                        "entityType": "MenuItem",
                        "externalId": null,
                        "images": [
                          {
                            "url": "https://toast-stage.s3.amazonaws.com/restaurants/restaurant-5268000000000000/menu/items/5/item-5740001516329005_1510177487.jpg"
                          }
                        ],
                        "visibility": "ALL",
                        "unitOfMeasure": "NONE",
                        "optionGroups": [
                          {
                            "guid": "34aa73d3-996b-43ff-87aa-b7283e4e417f",
                            "entityType": "MenuOptionGroup",
                            "externalId": null
                          },
                          {
                            "guid": "2f99aa77-869c-4267-aa99-179fc41e4562",
                            "entityType": "MenuOptionGroup",
                            "externalId": null
                          }
                        ],
                        "calories": null,
                        "type": null,
                        "inheritUnitOfMeasure": true,
                        "inheritOptionGroups": false,
                        "orderableOnline": "Yes",
                        "name": "Big Bacon Bowl",
                        "plu": "516329006",
                        "sku": "804768"
                      },
                      {
                        "guid": "9edf1418-b580-430c-a733-14a5bcaa22ba",
                        "entityType": "MenuItem",
                        "externalId": null,
                        "images": [],
                        "visibility": "ALL",
                        "unitOfMeasure": "NONE",
                        "optionGroups": [],
                        "calories": null,
                        "type": null,
                        "inheritUnitOfMeasure": true,
                        "inheritOptionGroups": true,
                        "orderableOnline": "Yes",
                        "name": "Maple Syrup",
                        "plu": "659648980",
                        "sku": "804814"
                      }
                      ]
                   """,
                "menu-items",
                "2022-02-24"
            )
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "menu_items"
        df.createOrReplaceTempView(tableName)

        // when
        val actual = MenuItemsProcessor.readMenuItems(spark, "2022-02-24", tableName)

        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "item_id",
                "item_nm",
                "item_plu",
                "variation_array"
            )
        }
        val actualToastData = actual.collect()
        withClue("POS Toast refined data do not match") {
            val expected =
                List(
                    (
                        "9ea8c952-1b18-431e-b179-96159244a3ec",
                        "Big Bacon Bowl",
                        "516329006",
                        Array("34aa73d3-996b-43ff-87aa-b7283e4e417f", "2f99aa77-869c-4267-aa99-179fc41e4562")
                    ),
                    ("9edf1418-b580-430c-a733-14a5bcaa22ba", "Maple Syrup", "659648980", Array.empty[String])
                ).toDF("item_id", "item_nm", "item_plu", "variation_array").collect()
            actualToastData.length should equal(expected.length)
            actualToastData should contain theSameElementsAs expected
        }
    }

    test("test readMenuOptionGroups") {
        // given
        import spark.implicits._
        val df = List(
            (
                s"""
                   |[
                   |  {
                   |    "guid": "86c0f378-e50f-475f-87d0-fbbe9118c40a",
                   |    "entityType": "MenuOptionGroup",
                   |    "externalId": null,
                   |    "name": "NO",
                   |    "options": [
                   |      {
                   |        "guid": "70cfc874-4cea-4403-b915-69e26c29e89c",
                   |        "entityType": "MenuOption",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "fc625933-1dcb-40e0-a22d-6f081b01cfea",
                   |        "entityType": "MenuOption",
                   |        "externalId": null
                   |      }
                   |    ],
                   |    "minSelections": null,
                   |    "maxSelections": null
                   |  },
                   |  {
                   |    "guid": "86eab1e4-8541-40f5-87f8-42d839c6b58a",
                   |    "entityType": "MenuOptionGroup",
                   |    "externalId": null,
                   |    "name": "Sauce Choice",
                   |    "options": [],
                   |    "minSelections": null,
                   |    "maxSelections": null
                   |  },
                   |  {
                   |    "guid": "8764bd57-5eab-49bd-be8b-1d0906fceb74",
                   |    "entityType": "MenuOptionGroup",
                   |    "externalId": null,
                   |    "name": "Please Remove",
                   |    "options": [
                   |      {
                   |        "guid": "acfa9dec-bef1-4792-9951-aae310106caf",
                   |        "entityType": "MenuOption",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "2304d2a1-ec75-4fcb-92fa-1287cc514677",
                   |        "entityType": "MenuOption",
                   |        "externalId": null
                   |      }
                   |    ],
                   |    "minSelections": null,
                   |    "maxSelections": null
                   |  }
                   | ]
                   |""".stripMargin,
                "menu-option-groups",
                "2022-02-24"
            )
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "menu_option_groups"
        df.createOrReplaceTempView(tableName)

        // when
        val actual = MenuItemsProcessor.readMenuOptionGroups(spark, "2022-02-24", tableName)

        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "item_id",
                "item_nm"
            )
        }
        val actualToastData = actual.collect()
        withClue("POS Toast refined data do not match") {
            val expected =
                List(
                    ("86c0f378-e50f-475f-87d0-fbbe9118c40a", "NO"),
                    ("86eab1e4-8541-40f5-87f8-42d839c6b58a", "Sauce Choice"),
                    ("8764bd57-5eab-49bd-be8b-1d0906fceb74", "Please Remove")
                ).toDF("item_id", "item_nm").collect()
            actualToastData should contain theSameElementsAs expected
        }
    }

    test("test readMenuGroups") {
        // given
        import spark.implicits._
        val df = List(
            (
                s"""
                   |[
                   |  {
                   |    "guid": "5dd0db99-d47e-4913-a039-1d59924d5329",
                   |    "entityType": "MenuGroup",
                   |    "externalId": null,
                   |    "parent": null,
                   |    "images": [],
                   |    "visibility": "ALL",
                   |    "unitOfMeasure": "NONE",
                   |    "optionGroups": [
                   |      {
                   |        "guid": "04868e23-3e24-474d-b660-67d5043b8b27",
                   |        "entityType": "MenuOptionGroup",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "005b8293-c9c8-4ed6-8c97-1b2564e52836",
                   |        "entityType": "MenuOptionGroup",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "4bcba811-d0b1-43ad-9cfc-a60a87a06fa6",
                   |        "entityType": "MenuOptionGroup",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "46ecd621-1b87-458f-84e2-b3b54569eea7",
                   |        "entityType": "MenuOptionGroup",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "adc3c630-4bdc-430a-ae6e-9e47ccda0876",
                   |        "entityType": "MenuOptionGroup",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "34aa73d3-996b-43ff-87aa-b7283e4e417f",
                   |        "entityType": "MenuOptionGroup",
                   |        "externalId": null
                   |      }
                   |    ],
                   |    "menu": null,
                   |    "inheritUnitOfMeasure": true,
                   |    "subgroups": [
                   |      {
                   |        "guid": "0f276491-4610-473c-9753-03079ad164e9",
                   |        "entityType": "MenuGroup",
                   |        "externalId": null
                   |      },
                   |      {
                   |        "guid": "2f01e1ac-2c7d-4262-9bb5-df1cd130e155",
                   |        "entityType": "MenuGroup",
                   |        "externalId": null
                   |      }
                   |    ],
                   |    "inheritOptionGroups": true,
                   |    "orderableOnline": "Yes",
                   |    "name": "Scrambles",
                   |    "items": []
                   |  },
                   |  {
                   |    "guid": "5e09fae8-43a3-46bd-bcc6-7338505cb3f8",
                   |    "entityType": "MenuGroup",
                   |    "externalId": null,
                   |    "parent": {
                   |      "guid": "45041402-88cd-4a87-b521-96f2b61adc93",
                   |      "entityType": "MenuGroup",
                   |      "externalId": null
                   |    },
                   |    "images": [],
                   |    "visibility": "ALL",
                   |    "unitOfMeasure": "NONE",
                   |    "optionGroups": [],
                   |    "menu": null,
                   |    "inheritUnitOfMeasure": true,
                   |    "subgroups": [],
                   |    "inheritOptionGroups": true,
                   |    "orderableOnline": "Yes",
                   |    "name": "Bowl",
                   |    "items": [
                   |      {
                   |        "guid": "86eab1e4-8541-40f5-87f8-42d839c6b58a"
                   |      }
                   |    ]
                   |  }
                   |]
                   |""".stripMargin,
                "menu-groups",
                "2022-02-24"
            )
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "menu_groups"
        df.createOrReplaceTempView(tableName)

        // when
        val actual = MenuItemsProcessor.readMenuGroups(spark, "2022-02-24", tableName)

        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "variation_array",
                "category_array",
                "items_ids",
                "main_category_name"
            )
        }
        val actualToastData = actual.collect()
        withClue("POS Toast refined data do not match") {
            val expected =
                List(
                    (
                        Array(
                            "04868e23-3e24-474d-b660-67d5043b8b27",
                            "005b8293-c9c8-4ed6-8c97-1b2564e52836",
                            "4bcba811-d0b1-43ad-9cfc-a60a87a06fa6",
                            "46ecd621-1b87-458f-84e2-b3b54569eea7",
                            "adc3c630-4bdc-430a-ae6e-9e47ccda0876",
                            "34aa73d3-996b-43ff-87aa-b7283e4e417f"
                        ),
                        Array("0f276491-4610-473c-9753-03079ad164e9", "2f01e1ac-2c7d-4262-9bb5-df1cd130e155"),
                        Array.empty[String],
                        "Scrambles"
                    ),
                    (Array.empty[String], Array.empty[String], Array("86eab1e4-8541-40f5-87f8-42d839c6b58a"), "Bowl")
                ).toDF("variation_array", "category_array", "items_ids", "main_category_name").collect()
            actualToastData should contain theSameElementsAs expected
        }

    }

}

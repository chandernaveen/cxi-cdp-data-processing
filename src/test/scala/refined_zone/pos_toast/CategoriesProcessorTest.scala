package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class CategoriesProcessorTest extends BaseSparkBatchJobTest {

    test("test toast partner category read") {
        // given
        import spark.implicits._
        val categories = List(
            (
                s"""
                   |{
                   |   "restaurantGuid":"f11f761e-2c7a-45e9-9727-e3a53a5b2be0",
                   |   "lastUpdated":"2022-05-31T13:25:44.572+0000",
                   |   "restaurantTimeZone":"America/Chicago",
                   |   "menus":[
                   |      {
                   |         "name":"Shakes",
                   |         "guid":"63d12c97-3936-4c10-9f29-321701606f79",
                   |         "menuGroups":[
                   |            {
                   |               "name":"Regular",
                   |               "guid":"bead2fdb-4a96-4b5b-9f75-59b11a7f825e",
                   |               "menuItems":[
                   |                  {
                   |                     "name":"Basic Beach",
                   |                     "guid":"6f3c9222-5d98-4ea9-a8db-91ce6f613877",
                   |                     "salesCategory":{
                   |                        "name":"Drinks/Juices",
                   |                        "guid":"f02adc77-27e5-43f5-8b68-b1fd7fca19c1"
                   |                     }
                   |                  },
                   |                  {
                   |                     "name":"Avo Matcha",
                   |                     "guid":"8e1c8122-69b3-4695-9230-8a6392994802",
                   |                     "salesCategory":{
                   |                        "name":"Drinks/Juices",
                   |                        "guid":"f02adc77-27e5-43f5-8b68-b1fd7fca19c1"
                   |                     }
                   |                  }
                   |                ]
                   |              }
                   |          ]
                   |      }
                   |      ]
                   |      }
                   |
                   |""".stripMargin,
                "menus",
                "2022-02-24"
            )
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "categories"
        categories.createOrReplaceTempView(tableName)

        // when
        val actual = CategoriesProcessor.readCategories(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cat_id", "cat_nm", "location_id")
        }
        val actualToastRefinedCategoriesData = actual.collect()
        withClue("POS Toast refined categories data do not match") {
            val expected = List(
                ("f02adc77-27e5-43f5-8b68-b1fd7fca19c1", "Drinks/Juices", "f11f761e-2c7a-45e9-9727-e3a53a5b2be0"),
                ("f02adc77-27e5-43f5-8b68-b1fd7fca19c1", "Drinks/Juices", "f11f761e-2c7a-45e9-9727-e3a53a5b2be0")
            ).toDF("cat_id", "cat_nm", "location_id").collect()
            actualToastRefinedCategoriesData.length should equal(expected.length)
            actualToastRefinedCategoriesData should contain theSameElementsAs expected
        }
    }

    test("test toast partner category transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val categories = List(
            ("f02adc77-27e5-43f5-8b68-b1fd7fca19c1", "Drinks/Juices", "f11f761e-2c7a-45e9-9727-e3a53a5b2be0"),
            (
                "f02adc77-27e5-43f5-8b68-b1fd7fca19c1",
                "Drinks/Juices",
                "f11f761e-2c7a-45e9-9727-e3a53a5b2be0"
            ), // duplicate
            ("d783e4b0-1185-4dab-a41e-343b029fbe19", "Food", "f11f761e-2c7a-45e9-9727-e3a53a5b2be0")
        ).toDF("cat_id", "cat_nm", "location_id")

        // when
        val actual = CategoriesProcessor.transformCategories(categories, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cat_id", "cat_nm", "location_id", "cxi_partner_id", "cat_desc")
        }
        val actualToastRefinedCategoriesData = actual.collect()
        withClue("POS Toast refined categories data do not match") {
            val expected = List(
                (
                    "f02adc77-27e5-43f5-8b68-b1fd7fca19c1",
                    "Drinks/Juices",
                    "f11f761e-2c7a-45e9-9727-e3a53a5b2be0",
                    cxiPartnerId,
                    null
                ),
                (
                    "d783e4b0-1185-4dab-a41e-343b029fbe19",
                    "Food",
                    "f11f761e-2c7a-45e9-9727-e3a53a5b2be0",
                    cxiPartnerId,
                    null
                )
            ).toDF("cat_id", "cat_nm", "location_id", "cxi_partner_id", "cat_desc").collect()
            actualToastRefinedCategoriesData.length should equal(expected.length)
            actualToastRefinedCategoriesData should contain theSameElementsAs expected
        }
    }

}

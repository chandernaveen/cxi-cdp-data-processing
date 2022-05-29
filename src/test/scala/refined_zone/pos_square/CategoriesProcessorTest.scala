package com.cxi.cdp.data_processing
package refined_zone.pos_square

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class CategoriesProcessorTest extends BaseSparkBatchJobTest {

    test("test square partner category read") {
        // given
        import spark.implicits._
        val categories = List(
            (
                s"""
                   {
                     "id": "IAVONRKHRTYYU7AP7EI57LA3",
                     "category_data": {
                        "name": "Event"
                     },
                     "type": "CATEGORY"
                   }
                   """,
                "objects",
                "2021-10-11"
            ),
            (
                s"""
                   {
                     "id": "IAVONRKHRTYYU7AP7EI57LA3",
                     "category_data": {
                        "name": "Event"
                     },
                     "type": "CATEGORY"
                   }
                   """,
                "objects",
                "2021-10-11"
            ), // duplicate
            (
                s"""
                   {
                     "id": "K67IQAEQQS6HHINURAXXWEVG",
                     "category_data": {
                        "name": "Drinks"
                     },
                     "type": "CATEGORY"
                   }
                   """,
                "objects",
                "2021-10-10"
            ),
            (
                s"""
                   {
                     "id": "3",
                     "type": "ITEM"
                   }
                   """,
                "objects",
                "2021-10-11"
            )
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "categories"
        categories.createOrReplaceTempView(tableName)

        // when
        val actual = CategoriesProcessor.readCategories(spark, "2021-10-11", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cat_id", "cat_nm")
        }
        val actualSquareRefinedCategoriesData = actual.collect()
        withClue("POS Square refined categories data do not match") {
            val expected = List(
                ("IAVONRKHRTYYU7AP7EI57LA3", "Event"),
                ("IAVONRKHRTYYU7AP7EI57LA3", "Event")
            ).toDF("cat_id", "cat_nm").collect()
            actualSquareRefinedCategoriesData.length should equal(expected.length)
            actualSquareRefinedCategoriesData should contain theSameElementsAs expected
        }
    }

    test("test square partner category transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val categories = List(
            ("1", "Food"),
            ("1", "Food"), // duplicate
            ("2", "Liquor"),
            ("3", "Beer")
        ).toDF("cat_id", "cat_nm")

        // when
        val actual = CategoriesProcessor.transformCategories(categories, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cat_id", "cat_nm", "cxi_partner_id", "cat_desc", "location_id")
        }
        val actualSquareRefinedCategoriesData = actual.collect()
        withClue("POS Square refined categories data do not match") {
            val expected = List(
                ("1", "Food", cxiPartnerId, null, null),
                ("2", "Liquor", cxiPartnerId, null, null),
                ("3", "Beer", cxiPartnerId, null, null)
            ).toDF("cat_id", "cat_nm", "cxi_partner_id", "cat_desc", "location_id").collect()
            actualSquareRefinedCategoriesData.length should equal(expected.length)
            actualSquareRefinedCategoriesData should contain theSameElementsAs expected
        }
    }

}

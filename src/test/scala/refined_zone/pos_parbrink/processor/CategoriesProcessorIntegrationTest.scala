package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.processor.CategoriesProcessorTest.CategoryRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class CategoriesProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_categories_processor")

    test("write Parbrink categories") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val category_A = CategoryRefined(
            cat_id = "1",
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            cat_nm = "Plates",
            cat_desc = null
        )
        val categories_1 = Seq(category_A).toDF()

        // when
        // write categories_1 first time
        CategoriesProcessor.writeCategories(categories_1, cxiPartnerId, destTable)

        // then
        withClue("Saved categories do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", categories_1.columns.size, actual.columns.size)
            assertDataFrameEquals(categories_1, actual)
        }

        // when
        // write categories_1 one more time
        CategoriesProcessor.writeCategories(categories_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved categories do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", categories_1.columns.size, actual.columns.size)
            assertDataFrameEquals(categories_1, actual)
        }

        // given
        val category_A_modified = category_A.copy(cat_nm = "category A name updated")
        val category_B = CategoryRefined(
            cat_id = "2",
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            cat_nm = "Beverages",
            cat_desc = null
        )
        val categories_2 = Seq(category_A_modified, category_B).toDF()

        // when
        // write modified category_A and new category_B
        CategoriesProcessor.writeCategories(categories_2, cxiPartnerId, destTable)

        // then
        // category_A updated, category_B added
        withClue("Saved categories do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", categories_2.columns.size, actual.columns.size)
            assertDataFrameEquals(categories_2, actual)
        }

    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(destTable)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(destTable)
    }

    def createTempTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE $tableName
               |(
               |    `cat_id`         STRING,
               |    `cxi_partner_id` STRING,
               |    `location_id`    STRING,
               |    `cat_nm`         STRING,
               |    `cat_desc`       STRING
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.processor.MenuItemsProcessorIntegrationTest.MenuItemRefined
import refined_zone.pos_parbrink.processor.MenuItemsProcessorTest.MenuItemTransformed
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class MenuItemsProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_menu_items_processor")

    test("write Parbrink menu items") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val menuItem_A = MenuItemTransformed(
            item_id = "1",
            item_nm = "Sauce",
            item_type = "Normal",
            item_plu = "code1",
            cxi_partner_id = cxiPartnerId,
            item_desc = null,
            item_barcode = null,
            variation_array = Seq(),
            category_array = Seq("11", "22"),
            main_category_name = "22"
        )

        val menuItems_1 = Seq(menuItem_A).toDF()

        // when
        // write menuItems_1 first time
        MenuItemsProcessor.writeMenuItems(menuItems_1, cxiPartnerId, destTable)

        // then
        val menuItem_A_Refined = MenuItemRefined(
            item_id = "1",
            item_nm = "Sauce",
            item_type = "Normal",
            item_plu = "code1",
            cxi_partner_id = cxiPartnerId,
            item_desc = null,
            item_barcode = null,
            variation_array = Seq(),
            category_array = Seq("11", "22"),
            main_category_name = "22"
        )
        withClue("Saved menuItems do not match") {

            val actual = spark.table(destTable)
            val expected = Seq(menuItem_A_Refined).toDF()
            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }

        // when
        // write menuItems_1 one more time
        MenuItemsProcessor.writeMenuItems(menuItems_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved menuItems do not match") {
            val actual = spark.table(destTable)
            val expected = Seq(menuItem_A_Refined).toDF()
            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }

        // given
        val menuItem_A_modified = menuItem_A.copy(item_plu = "modified")
        val menuItem_B = MenuItemTransformed(
            item_id = "3",
            item_nm = "Pizza",
            item_type = "Composite",
            item_plu = null,
            cxi_partner_id = cxiPartnerId,
            item_desc = null,
            item_barcode = null,
            variation_array = Seq(1, 2),
            category_array = null,
            main_category_name = null
        )
        val menuItems_2 = Seq(menuItem_A_modified, menuItem_B).toDF()

        // when
        // write modified menuItem_A and new menuItem_B
        MenuItemsProcessor.writeMenuItems(menuItems_2, cxiPartnerId, destTable)

        // then
        // menuItem_A updated, menuItem_B added
        val menuItem_A_Refined_Modified = menuItem_A_Refined.copy(item_plu = "modified")

        val menuItem_B_Refined = MenuItemRefined(
            item_id = "3",
            item_nm = "Pizza",
            item_type = "Composite",
            item_plu = null,
            cxi_partner_id = cxiPartnerId,
            item_desc = null,
            item_barcode = null,
            variation_array = Seq("1", "2"),
            category_array = null,
            main_category_name = null
        )
        withClue("Saved menuItems do not match") {
            val actual = spark.table(destTable)
            val expected = Seq(menuItem_A_Refined_Modified, menuItem_B_Refined).toDF()
            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
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
               |    `item_id`            STRING,
               |    `cxi_partner_id`     STRING,
               |    `item_nm`            STRING,
               |    `item_desc`          STRING,
               |    `item_type`          STRING,
               |    `category_array`     ARRAY<STRING>,
               |    `main_category_name` STRING,
               |    `variation_array`    ARRAY<STRING>,
               |    `item_plu`           STRING,
               |    `item_barcode`       STRING
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

object MenuItemsProcessorIntegrationTest {

    case class MenuItemRefined(
        item_id: String,
        item_nm: String,
        item_type: String,
        item_plu: String,
        cxi_partner_id: String,
        item_desc: String,
        item_barcode: String,
        variation_array: Seq[String],
        category_array: Seq[String],
        main_category_name: String
    )

}

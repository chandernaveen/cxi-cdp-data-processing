package com.cxi.cdp.data_processing
package refined_zone.pos_square

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class MenuItemsProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("test add variations to menu items") {
        // given
        val menuItems = Seq(
            ("item_1", "Combo Bop", "Two choices of meat", "ITEM", "cat1",
                """[
                  |  {
                  |    "id": "var_1",
                  |    "is_deleted": false,
                  |    "item_variation_data": {
                  |      "item_id": "item_1",
                  |      "location_overrides": [],
                  |      "name": "Regular",
                  |      "ordinal": 1,
                  |      "price_money": {
                  |        "amount": 945,
                  |        "currency": "USD"
                  |      },
                  |      "pricing_type": "FIXED_PRICING",
                  |      "sellable": true,
                  |      "service_duration": 0,
                  |      "stockable": true
                  |    },
                  |    "present_at_all_locations": true,
                  |    "type": "ITEM_VARIATION",
                  |    "updated_at": "2021-10-07T02:33:54.758Z",
                  |    "version": 1633574034758
                  |  }
                  |]""".stripMargin)
        ).toDF("item_id", "item_nm", "item_desc", "item_type", "category_array", "variations")

        val itemsVariations = Seq(
            ("item_1",
                """[
                  |  {
                  |    "id": "var_1",
                  |    "is_deleted": false,
                  |    "item_variation_data": {
                  |      "item_id": "item_1",
                  |      "location_overrides": [],
                  |      "name": "Regular",
                  |      "ordinal": 1,
                  |      "price_money": {
                  |        "amount": 945,
                  |        "currency": "USD"
                  |      },
                  |      "pricing_type": "FIXED_PRICING",
                  |      "sellable": true,
                  |      "service_duration": 0,
                  |      "stockable": true
                  |    },
                  |    "present_at_all_locations": true,
                  |    "type": "ITEM_VARIATION",
                  |    "updated_at": "2021-10-07T02:33:54.758Z",
                  |    "version": 1633574034758
                  |  }
                  |]""".stripMargin)
        )
            .toDF("item_id", "variations")

        // when
        val result = MenuItemsProcessor.transformMenuItems(menuItems, itemsVariations, "test-partner").collect()

        // then
        val expected = Seq(
            ("item_1", "Combo Bop", "Two choices of meat", "food", "cat1", Array("var_1"), "test-partner", null, null, null),
            ("var_1", "Regular", null, "variation", null, null, "test-partner", null, null, null)
        ).toDF("item_id", "item_nm", "item_desc", "item_type", "category_array", "variation_array", "cxi_partner_id",
            "main_category_name", "item_plu", "item_barcode").collect()
        result should contain theSameElementsAs expected
    }

}

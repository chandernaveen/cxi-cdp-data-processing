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

    test("test process menu items") {
        // given
        val scrTable = "test_menuitemprocessor_src"
        val cxiPartnerId = "testPartner"
        val feedDate = "2021-11-02"

        val recordValue = """{
                            |  "id": "34LZBM2A3JNZ5MHWW3QLKMXU",
                            |  "image_id": "B4BLB36ZUVFMYB4GROQT27JI",
                            |  "is_deleted": false,
                            |  "item_data": {
                            |    "available_electronically": false,
                            |    "available_for_pickup": false,
                            |    "available_online": false,
                            |    "category_id": "UOZ26MXLICMNYJ5VKH6SHJSV",
                            |    "description": "Two choices of meat. Served with rice, cabbage mix and noodle.  ",
                            |    "ecom_available": true,
                            |    "ecom_image_uris": [
                            |      "https://cupbop.square.site/uploads/1/3/1/3/131366937/s307059117523367604_p5_i3_w533.jpeg"
                            |    ],
                            |    "ecom_visibility": "HIDDEN",
                            |    "name": "Combo Bop",
                            |    "ordinal": 0,
                            |    "product_type": "REGULAR",
                            |    "skip_modifier_screen": false,
                            |    "tax_ids": [
                            |      "WKLDGAE7WMO4LOERRAVNI3B2",
                            |      "C7KA7TNNHRM3WMFFU5FPHTXG",
                            |      "NUYWQ2PP3D4NA4JW3Z4SQHLI"
                            |    ],
                            |    "variations": [
                            |      {
                            |        "id": "YY7ZT6JP2YTZK2DXZNLUSW5S",
                            |        "is_deleted": false,
                            |        "item_variation_data": {
                            |          "item_id": "34LZBM2A3JNZ5MHWW3QLKMXU",
                            |          "name": "Regular",
                            |          "ordinal": 1,
                            |          "price_money": {
                            |            "amount": 945,
                            |            "currency": "USD"
                            |          },
                            |          "pricing_type": "FIXED_PRICING",
                            |          "sellable": true,
                            |          "service_duration": 0,
                            |          "stockable": true
                            |        },
                            |        "present_at_all_locations": true,
                            |        "type": "ITEM_VARIATION",
                            |        "updated_at": "2021-10-07T02:33:54.758Z",
                            |        "version": 1633574034758
                            |      }
                            |    ],
                            |    "visibility": "PRIVATE"
                            |  },
                            |  "present_at_all_locations": true,
                            |  "type": "ITEM",
                            |  "updated_at": "2021-10-07T02:33:54.758Z",
                            |  "version": 1633574034758
                            |}""".stripMargin

        val rawData = Seq(("objects", recordValue, "CAASMwoTNTY4NjgyMjE5OjY2ODQ4OTI5NxIcEAEQBBAGEAgQChAhECIQIxAlECYQJxApEC04ZA", feedDate,
            "catalog_list_637716625269333561_4dbb5934-6580-4da4-b149-227603043c16", "c8118a58-7aef-45f5-bba6-a03faa63eb8e"))
            .toDF("record_type", "record_value", "cursor", "feed_date", "file_name", "cxi_id")
        rawData.createOrReplaceTempView(scrTable)

        // when
        val result = MenuItemsProcessor.buildMenuItems(spark, cxiPartnerId, feedDate, scrTable).collect()

        // then
        val expected = Seq(
            ("34LZBM2A3JNZ5MHWW3QLKMXU", "Combo Bop", "Two choices of meat. Served with rice, cabbage mix and noodle.  ", "food","UOZ26MXLICMNYJ5VKH6SHJSV",
                Array("YY7ZT6JP2YTZK2DXZNLUSW5S"), "testPartner", null, null, null),
            ("YY7ZT6JP2YTZK2DXZNLUSW5S", "Regular", null, "variation", null, null, "testPartner", null, null, null)
        ).toDF("item_id", "item_nm", "item_desc", "item_type", "category_array", "variation_array", "cxi_partner_id",
            "main_category_name", "item_plu", "item_barcode").collect()

        result should contain theSameElementsAs expected
    }

}

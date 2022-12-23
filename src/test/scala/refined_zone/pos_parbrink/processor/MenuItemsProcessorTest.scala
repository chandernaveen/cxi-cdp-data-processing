package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.model.ParbrinkRawModels.IncludedModifier
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.CategoriesProcessorTest.{CategoryRaw, ItemRaw}
import refined_zone.pos_parbrink.processor.MenuItemsProcessorTest._
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class MenuItemsProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val includedModifiers = Seq(
        IncludedModifier(
            AutomaticallyAdd = true,
            IsIncluded = true,
            ItemId = 1,
            ModifierGroupId = 0,
            Position = 0,
            PrintInKitchen = true
        ),
        IncludedModifier(
            AutomaticallyAdd = true,
            IsIncluded = true,
            ItemId = 2,
            ModifierGroupId = 0,
            Position = 0,
            PrintInKitchen = true
        )
    )

    val menuItem_1_onRead =
        MenuItemOnRead(location_id = "loc_id_1", item_id = 1, item_nm = "Sauce", item_type = 0, item_plu = "code1")

    val menuItem_2_onRead =
        MenuItemOnRead(location_id = "loc_id_1", item_id = 2, item_nm = "Bacon", item_type = 0)

    val menuItem_3_onRead =
        MenuItemOnRead(
            location_id = "loc_id_1",
            item_id = 3,
            item_nm = "Pizza",
            item_type = 4,
            included_modifiers = Serialization.write(includedModifiers)
        )

    test("read Parbrink menu items") {

        // given
        val item_1 = MenuItemRaw(Id = 1, Name = "Sauce", ItemType = 0, PLU = "code1")
        val item_2 = MenuItemRaw(Id = 2, Name = "Bacon", ItemType = 0)
        val item_3 = MenuItemRaw(Id = 3, Name = "Pizza", ItemType = 4, IncludedModifiers = includedModifiers)
        val item_4 = MenuItemRaw(
            Id = 4,
            Name = "Chile Verde Pork",
            ItemType = 0,
            IsDeleted = true
        ) // IsDeleted = true, should be ignored

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(item_1),
                record_type = ParbrinkRecordType.MenuItems.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/items_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(item_1),
                record_type = ParbrinkRecordType.MenuItems.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/items_2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(item_2),
                record_type = ParbrinkRecordType.MenuItems.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/items_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(item_3),
                record_type = ParbrinkRecordType.MenuItems.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/items_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = "orders", // not a category type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(item_3),
                record_type = ParbrinkRecordType.MenuItems.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/items_1.json",
                cxi_id = "cxi_id_4"
            ),
            ParbrinkRawZoneModel( // deleted item, should be ignored
                record_value = Serialization.write(item_4),
                record_type = ParbrinkRecordType.MenuItems.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/items_1.json",
                cxi_id = "cxi_id_1"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val menuItemsOnRead = MenuItemsProcessor.readMenuItems(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink menu items data does not match") {
            val expected = Seq(
                menuItem_1_onRead,
                menuItem_1_onRead, // emulate duplicated data coming from different files
                menuItem_2_onRead,
                menuItem_3_onRead
            ).toDF()

            assert("Column size not Equal", expected.columns.size, menuItemsOnRead.columns.size)
            assertDataFrameDataEquals(expected, menuItemsOnRead)
        }
    }

    test("read and transform Parbrink categories for menu items") {

        // given
        val category_1 = CategoryRaw(Id = 11, IsDeleted = false, Items = Seq(ItemRaw(1)), Name = "All Items")
        val category_2 = CategoryRaw(Id = 22, IsDeleted = false, Items = Seq(ItemRaw(1), ItemRaw(2)), Name = "Plates")
        val category_3 = CategoryRaw( // IsDeleted = true, should be ignored
            Id = 33,
            IsDeleted = true,
            Items = Seq(ItemRaw(1), ItemRaw(2), ItemRaw(3)),
            Name = "C* Appetizers"
        )

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(category_1),
                record_type = ParbrinkRecordType.Categories.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/itemGroups_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(category_1),
                record_type = ParbrinkRecordType.Categories.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/itemGroups_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(category_2),
                record_type = ParbrinkRecordType.Categories.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/itemGroups_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(category_3),
                record_type = ParbrinkRecordType.Categories.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/itemGroups_1.json",
                cxi_id = "cxi_id_1"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val categoriesOnRead = MenuItemsProcessor.readCategories(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink categories data does not match") {
            val category_1_onRead =
                CategoryOnRead(location_id = "loc_id_1", cat_id = 11, item_array = Serialization.write(Seq(ItemRaw(1))))
            val category_2_onRead =
                CategoryOnRead(
                    location_id = "loc_id_1",
                    cat_id = 22,
                    item_array = Serialization.write(Seq(ItemRaw(1), ItemRaw(2)))
                )
            val expected = Seq(
                category_1_onRead,
                category_1_onRead, // emulate duplicated data coming from different files
                category_2_onRead
            ).toDF()

            assert("Column size not Equal", expected.columns.size, categoriesOnRead.columns.size)
            assertDataFrameDataEquals(expected, categoriesOnRead)
        }

        // when
        val transformedCategories = MenuItemsProcessor.transformCategories(categoriesOnRead)

        // then
        withClue("transformed Parbrink categories data does not match") {
            val expected = Seq(
                CategoryTransformed("loc_id_1", "1", Seq("11", "22")),
                CategoryTransformed("loc_id_1", "2", Seq("22"))
            ).toDF()

            assert("Column size not Equal", expected.columns.size, transformedCategories.columns.size)
            assertDataFrameDataEquals(expected, transformedCategories)
        }
    }

    test("transform Parbrink menu items") {

        // given
        val menuItemsOnRead = Seq(
            menuItem_1_onRead,
            menuItem_1_onRead, // emulate duplicated data coming from different files
            menuItem_2_onRead,
            menuItem_3_onRead
        ).toDF()

        val transformedCategories = Seq(
            CategoryTransformed("loc_id_1", "1", Seq("11", "22")),
            CategoryTransformed("loc_id_1", "2", Seq("22"))
        ).toDF()

        // when
        val transformedMenuItems =
            MenuItemsProcessor.transformMenuItems(menuItemsOnRead, transformedCategories, cxiPartnerId)

        // then
        withClue("transformed Parbrink menu items data does not match") {
            val expected = Seq(
                MenuItemTransformed(
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
                ),
                MenuItemTransformed(
                    item_id = "2",
                    item_nm = "Bacon",
                    item_type = "Normal",
                    item_plu = null,
                    cxi_partner_id = cxiPartnerId,
                    item_desc = null,
                    item_barcode = null,
                    variation_array = Seq(),
                    category_array = Seq("22"),
                    main_category_name = "22"
                ),
                MenuItemTransformed(
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
            ).toDF()

            assert("Column size not Equal", expected.columns.size, transformedMenuItems.columns.size)
            assertDataFrameDataEquals(expected, transformedMenuItems)
        }
    }

}

object MenuItemsProcessorTest {

    case class MenuItemRaw(
        Id: Int,
        Name: String,
        ItemType: Int,
        IsDeleted: Boolean = false,
        PLU: String = null,
        IncludedModifiers: Seq[IncludedModifier] = Seq()
    )

    case class MenuItemOnRead(
        location_id: String,
        item_id: Int,
        item_nm: String,
        item_type: Int,
        item_plu: String = null,
        included_modifiers: String = "[]"
    )

    case class CategoryOnRead(location_id: String, cat_id: Int, item_array: String)
    case class CategoryTransformed(location_id: String, item_id: String, category_array: Seq[String])

    case class MenuItemTransformed(
        item_id: String,
        item_nm: String,
        item_type: String,
        item_plu: String,
        cxi_partner_id: String,
        item_desc: String,
        item_barcode: String,
        variation_array: Seq[Int],
        category_array: Seq[String],
        main_category_name: String
    )
}

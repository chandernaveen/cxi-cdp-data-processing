package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.CategoriesProcessorTest.{
    CategoryOnRead,
    CategoryRaw,
    CategoryRefined,
    ItemRaw
}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class CategoriesProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val category_1_onRead = CategoryOnRead(location_id = "loc_id_1", cat_id = "1", cat_nm = "Plates")
    val category_2_onRead = CategoryOnRead(location_id = "loc_id_1", cat_id = "2", cat_nm = "Beverages")

    test("read Parbrink categories") {

        // given

        val category_1 = CategoryRaw(Id = 1, IsDeleted = false, Items = Seq(ItemRaw(100101)), Name = "Plates")
        val category_2 = CategoryRaw(Id = 2, IsDeleted = false, Items = Seq(ItemRaw(100102)), Name = "Beverages")
        val category_3 = CategoryRaw( // IsDeleted = true, should be ignored
            Id = 3,
            IsDeleted = true,
            Items = Seq(ItemRaw(100103)),
            Name = "Tacos"
        )
        val category_4 = CategoryRaw(Id = 4, IsDeleted = false, Items = Seq(ItemRaw(100104)), Name = "Nachos")

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
                file_name = "location=loc_id_1/itemGroups_2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel( // deleted category, should be ignored
                record_value = Serialization.write(category_3),
                record_type = ParbrinkRecordType.Categories.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/itemGroups_1.json",
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
                record_value = Serialization.write(category_4),
                record_type = ParbrinkRecordType.Categories.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/itemGroups_3.json",
                cxi_id = "cxi_id_4"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val categoriesOnRead = CategoriesProcessor.readCategories(spark, "2022-02-24", rawTable)

        // then
        withClue("read Parbrink categories data does not match") {
            val expected = Seq(
                category_1_onRead,
                category_1_onRead, // emulate duplicated data coming from different files
                category_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, categoriesOnRead.columns.size)
            assertDataFrameEquals(expected, categoriesOnRead)
        }
    }

    test("transform Parbrink categories") {

        // given
        val categoriesOnRead = Seq(
            category_1_onRead,
            category_1_onRead, // emulate duplicated data coming from different files
            category_2_onRead
        ).toDF()

        // when
        val transformedCategories = CategoriesProcessor.transformCategories(categoriesOnRead, cxiPartnerId)

        // then
        withClue("transformed Parbrink categories do not match") {
            val expected = Seq(
                CategoryRefined(
                    cat_id = "1",
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    cat_nm = "Plates",
                    cat_desc = null
                ),
                CategoryRefined(
                    cat_id = "2",
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    cat_nm = "Beverages",
                    cat_desc = null
                )
            ).toDF()
            assert("Column size not Equal", expected.columns.size, transformedCategories.columns.size)
            assertDataFrameDataEquals(expected, transformedCategories)
        }
    }

}

object CategoriesProcessorTest {

    case class ItemRaw(ItemId: Int)
    case class CategoryRaw(Id: Int, IsDeleted: Boolean, Items: Seq[ItemRaw], Name: String, AlternateId: String = null)
    case class CategoryOnRead(location_id: String, cat_id: String, cat_nm: String)

    case class CategoryRefined(
        cat_id: String,
        cxi_partner_id: String,
        location_id: String,
        cat_nm: String,
        cat_desc: String
    )
}

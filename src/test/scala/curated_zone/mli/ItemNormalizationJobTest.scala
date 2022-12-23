package com.cxi.cdp.data_processing
package curated_zone.mli

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

import java.sql.Timestamp
import java.time.Instant

class ItemNormalizationJobTest extends BaseSparkBatchJobTest {

    test("test get partner items to process") {
        // given
        import spark.implicits._
        val itemDf = List(
            ("item-id1", "cxi-partner-id1", "item-nm1", "food"),
            ("item-id2", "cxi-partner-id1", "item-nm1", "food"),
            ("item-id3", "cxi-partner-id1", "item-nm2", "variation"),
            ("item-id4", "cxi-partner-id1", "Small", "food"),
            ("item-id5", "cxi-partner-id1", "Extra Spice", "food"),
            ("item-id6", "cxi-partner-id2", "item-nm1", "food"),
            ("item-id7", "cxi-partner-id2", "30 oz", "food")
        ).toDF(
            "item_id",
            "cxi_partner_id",
            "item_nm",
            "item_type"
        )

        // when
        val actual = ItemNormalizationJob.getPartnerItemsToProcess(itemDf)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq(
                    "cxi_partner_id",
                    "item_nm"
                )
        }
        val actualPartnerItemData = actual.collect()
        withClue("Partner item data do not match") {
            val expected = List(
                ("cxi-partner-id1", "item-nm1"),
                ("cxi-partner-id2", "item-nm1")
            ).toDF(
                "cxi_partner_id",
                "item_nm"
            ).collect()
            actualPartnerItemData.length should equal(expected.length)
            actualPartnerItemData should contain theSameElementsAs expected
        }
    }

    test("test get partner items to process for empty change data") {
        import spark.implicits._
        val df = List.empty[(String, String, String)].toDF("cxi_partner_id", "item_nm", "item_type")

        ItemNormalizationJob.getPartnerItemsToProcess(df).count() shouldBe 0
    }

    test("test get partner item category") {
        // given
        import spark.implicits._

        val partnerItems = List(
            ("cxi-partner-id1", "item-nm1"),
            ("cxi-partner-id1", "item-nm2"),
            ("cxi-partner-id1", "item-nm3"),
            ("cxi-partner-id1", "dummy")
        ).toDF(
            "cxi_partner_id",
            "item_nm"
        )

        val itemCategoryTaxonomy = List(
            ("cxi-item-nm1", Array("cxi-item-category1")),
            ("item-nm", Array("cxi-item-category1", "cxi-item-category2")),
            ("cxi-item-nm3", Array("cxi-item-category3"))
        ).toDF(
            "cxi_item_nm",
            "cxi_item_categories"
        )

        // when
        val processTime = Timestamp.from(Instant.now())
        val actual = ItemNormalizationJob.getPartnerItemCategory(partnerItems, itemCategoryTaxonomy, processTime)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq(
                    "cxi_partner_id",
                    "cxi_item_category",
                    "pos_item_nm",
                    "updated_on"
                )
        }
        val partnerItemCategoryData = actual.collect()
        withClue("Item universe to update do not match") {
            val expected = List(
                ("cxi-partner-id1", "cxi-item-category1", "item-nm1", processTime),
                ("cxi-partner-id1", "cxi-item-category2", "item-nm1", processTime),
                ("cxi-partner-id1", "cxi-item-category1", "item-nm2", processTime),
                ("cxi-partner-id1", "cxi-item-category2", "item-nm2", processTime),
                ("cxi-partner-id1", "cxi-item-category1", "item-nm3", processTime),
                ("cxi-partner-id1", "cxi-item-category2", "item-nm3", processTime),
                ("cxi-partner-id1", "cxi-item-category3", "item-nm3", processTime),
                ("cxi-partner-id1", "Unknown", "dummy", processTime)
            ).toDF(
                "cxi_partner_id",
                "cxi_item_category",
                "pos_item_nm",
                "updated_on"
            ).collect()
            partnerItemCategoryData.length should equal(expected.length)
            partnerItemCategoryData should contain theSameElementsAs expected
        }
    }

}

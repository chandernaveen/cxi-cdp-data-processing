package com.cxi.cdp.data_processing
package refined_zone.pos_simphony

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class RawRefinedOracleSimphonyPartnerJobTest extends BaseSparkBatchJobTest {

    test("test oracle simphony partner category transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val categories = List(
            ("1", "P300002", "Food"),
            ("1", "P300002", "Food"), // duplicate
            ("2", "P300002", "Liquor"),
            ("3", "P300002", "Beer")
        ).toDF("cat_id", "location_id", "cat_nm")

        // when
        val actual = CategoriesProcessor.transformCategories(categories, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cat_id", "location_id", "cat_nm", "cxi_partner_id", "cat_desc")
        }
        val actualOracleSimRefinedCategoriesData = actual.collect()
        withClue("Oracle simphony refined categories data do not match") {
            val expected = List(
                ("1", "P300002", "Food", cxiPartnerId, null),
                ("2", "P300002", "Liquor", cxiPartnerId, null),
                ("3", "P300002", "Beer", cxiPartnerId, null)
            ).toDF("cat_id", "location_id", "cat_nm", "cxi_partner_id", "cat_desc").collect()
            actualOracleSimRefinedCategoriesData.length should equal(expected.length)
            actualOracleSimRefinedCategoriesData should contain theSameElementsAs expected
        }
    }

}

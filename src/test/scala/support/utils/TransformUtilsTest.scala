package com.cxi.cdp.data_processing
package support.utils

import support.BaseSparkBatchJobTest
import support.utils.TransformUtils.{CastDataType, ColumnsMapping, DestCol, SourceCol}

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class TransformUtilsTest extends BaseSparkBatchJobTest {

    test("test apply mappings and cast") {
        // given
        import spark.implicits._

        val sourceDf = Seq(
            ("cxi-usa-partner1", "partner 1", "10", "abcd"),
            ("cxi-usa-partner2", "partner 2", "20", "efgh")
        ).toDF("cxiPartnerId", "partner_name", "some_column_to_cast", "temp_col")

        val columnsMapping = ColumnsMapping(Seq(
            Map(SourceCol -> "cxiPartnerId", DestCol -> "cxi_partner_id"),
            Map(SourceCol -> "partner_name", DestCol -> "partner_nm"),
            Map(SourceCol -> "some_column_to_cast", DestCol -> "column_after_cast", CastDataType -> "long"))
        )

        // when
        val actual = TransformUtils.applyColumnMapping(sourceDf, columnsMapping, includeAllSourceColumns = false)

        // then
        val actualFieldsReturned = actual.schema.fields.map(_.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldBe Array("cxi_partner_id", "partner_nm", "column_after_cast")
        }
        val actualData = actual.collect()
        withClue("Transform data do not match") {
            val expected = Seq(
                ("cxi-usa-partner1", "partner 1", 10),
                ("cxi-usa-partner2", "partner 2", 20)
            ).toDF("cxi_partner_id", "partner_nm", "column_after_cast").collect()
            actualData.length should equal(expected.length)
            actualData should contain theSameElementsAs expected
        }
    }

    test("test apply mappings or take column as it is") {
        // given
        import spark.implicits._

        val sourceDf = Seq(
            ("cxi-usa-partner1", "partner 1", "10", "abcd"),
            ("cxi-usa-partner2", "partner 2", "20", "efgh")
        ).toDF("cxiPartnerId", "partner_name", "some_column_to_cast", "temp_col")

        val columnsMapping = ColumnsMapping(Seq(
            Map(SourceCol -> "cxiPartnerId", DestCol -> "cxi_partner_id"),
            Map(SourceCol -> "partner_name", DestCol -> "partner_nm"),
            Map(SourceCol -> "some_column_to_cast", DestCol -> "column_after_cast", CastDataType -> "long"))
        )

        // when
        val actual = TransformUtils.applyColumnMapping(sourceDf, columnsMapping, includeAllSourceColumns = true)

        // then
        val actualFieldsReturned = actual.schema.fields.map(_.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldBe Array("cxi_partner_id", "partner_nm", "column_after_cast", "temp_col")
        }
        val actualData = actual.collect()
        withClue("Transform data do not match") {
            val expected = Seq(
                ("cxi-usa-partner1", "partner 1", 10, "abcd"),
                ("cxi-usa-partner2", "partner 2", 20, "efgh")
            ).toDF("cxi_partner_id", "partner_nm", "column_after_cast", "temp_col").collect()
            actualData.length should equal(expected.length)
            actualData should contain theSameElementsAs expected
        }
    }

}

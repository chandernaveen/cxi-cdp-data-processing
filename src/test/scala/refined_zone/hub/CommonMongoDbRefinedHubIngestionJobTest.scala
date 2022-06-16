package com.cxi.cdp.data_processing
package refined_zone.hub

import support.utils.TransformUtils.{CastDataType, ColumnsMapping, DestCol, SourceCol}
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class CommonMongoDbRefinedHubIngestionJobTest extends BaseSparkBatchJobTest {

    test("test сonstruct join condition with one key") {
        // given
        val srcTable = "src_partner"
        val destTable = "dest_partner"
        val keys = List("cxi_partner_id")

        // when
        val actualJoinCondition = CommonMongoDbRefinedHubIngestionJob.constructJoinCondition(keys, srcTable, destTable)

        // then
        actualJoinCondition should equal(
            "dest_partner.feed_date <=> src_partner.feed_date AND dest_partner.cxi_partner_id <=> src_partner.cxi_partner_id"
        )
    }

    test("test сonstruct join condition with multiple keys") {
        // given
        val srcTable = "src_partner"
        val destTable = "dest_partner"
        val keys = List("mongo_object_id", "cxi_partner_id")

        // when
        val actualJoinCondition = CommonMongoDbRefinedHubIngestionJob.constructJoinCondition(keys, srcTable, destTable)

        // then
        actualJoinCondition should equal(
            "dest_partner.feed_date <=> src_partner.feed_date AND dest_partner.mongo_object_id <=> src_partner.mongo_object_id AND dest_partner.cxi_partner_id <=> src_partner.cxi_partner_id"
        )
    }

    test("test сonstruct columns that need to be updated") {
        // given
        val srcTable = "src_partner"
        val tableColumns = Array("cxi_partner_id", "partner_nm", "partner_type")

        // when
        val actualColumnsToBeUpdated =
            CommonMongoDbRefinedHubIngestionJob.constructColumnsToUpdate(tableColumns, srcTable)

        // then
        actualColumnsToBeUpdated should equal(
            "cxi_partner_id = src_partner.cxi_partner_id, partner_nm = src_partner.partner_nm, partner_type = src_partner.partner_type"
        )
    }

    test("test сonstruct columns that need to be inserted") {
        // given
        val srcTable = "src_partner"
        val tableColumns = Array("cxi_partner_id", "partner_nm", "partner_type")

        // when
        val actualColumnsToBeInserted =
            CommonMongoDbRefinedHubIngestionJob.constructColumnsToInsert(tableColumns, srcTable)

        // then
        actualColumnsToBeInserted should equal(
            "(cxi_partner_id, partner_nm, partner_type) VALUES (src_partner.cxi_partner_id, src_partner.partner_nm, src_partner.partner_type)"
        )
    }

    test("test mongo db refined hub ingestion transform") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val sourceCollectionDf = List(
            ("cxi-usa-partner1", "partner 1", "10"),
            ("cxi-usa-partner1", "partner 1", "10"), // duplicate
            ("cxi-usa-partner2", "partner 2", "20")
        ).toDF("cxiPartnerId", "partner_name", "some_column_to_cast")

        val columnsMapping = ColumnsMapping(
            List(
                Map(SourceCol -> "cxiPartnerId", DestCol -> "cxi_partner_id"),
                Map(SourceCol -> "partner_name", DestCol -> "partner_nm"),
                Map(SourceCol -> "some_column_to_cast", DestCol -> "column_after_cast", CastDataType -> "long")
            )
        )
        val feedDate = "2021-10-12"

        // when
        val actual = CommonMongoDbRefinedHubIngestionJob.transform(
            sourceCollectionDf,
            columnsMapping,
            List("cxi_partner_id"),
            feedDate,
            dropDuplicates = true
        )

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_partner_id", "partner_nm", "column_after_cast", "feed_date")
        }
        val actualData = actual.collect()
        withClue("Transform data do not match") {
            val expected = List(
                ("cxi-usa-partner1", "partner 1", 10, feedDate),
                ("cxi-usa-partner2", "partner 2", 20, feedDate)
            ).toDF("cxi_partner_id", "partner_nm", "column_after_cast", "feed_date").collect()
            actualData.length should equal(expected.length)
            actualData should contain theSameElementsAs expected
        }
    }

}

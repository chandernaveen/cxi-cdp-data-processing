package com.cxi.cdp.data_processing
package raw_zone

import raw_zone.PrivacyLookupDeduplicationJob.FeedDateToRunIdChangedData
import refined_zone.hub.identity.model.IdentityType
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class PrivacyLookupDeduplicationJobTest extends BaseSparkBatchJobTest {

    test("test getFeedDateToRunIdPairsToProcess") {
        // given
        val df = spark.createDataFrame(List(
            (sqlDate("2024-02-24"), "runId1"),
            (sqlDate("2024-02-24"), "runId1"), // duplicate
            (sqlDate("2024-02-24"), "runId2"),
            (sqlDate("2024-02-25"), "runId3")
        )).toDF("feed_date", "run_id")

        // when
        val feedDateToRunIdChangedDataSet = PrivacyLookupDeduplicationJob.getFeedDateToRunIdPairsToProcess(df)

        // then
        feedDateToRunIdChangedDataSet should contain theSameElementsAs Set(
            FeedDateToRunIdChangedData("2024-02-24", "runId1"),
            FeedDateToRunIdChangedData("2024-02-24", "runId2"),
            FeedDateToRunIdChangedData("2024-02-25", "runId3")
        )

    }

    test("test readPrivacyLookupIntermediateTable") {
        // given
        val tableName = "tempPrivacyLookupIntermediateTable"
        val cxiSource = "cxi-usa-goldbowl"
        val cxiSource2 = "cxi-usa-burgerking"
        val dateRaw = "2022-02-24"
        val dateRaw2 = "2022-02-25"
        val runId1 = "runId1"
        val runId2 = "runId2"
        val runId3 = "runId3"
        val feedDateToRunIdChangedDataSet = Set(FeedDateToRunIdChangedData(dateRaw, runId1), FeedDateToRunIdChangedData(dateRaw, runId2))
        val df = spark.createDataFrame(
            List(
                (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId2, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource2, sqlDate(dateRaw), runId3, "ov_3", "hv_3", IdentityType.Email.code), // excluded by run id
                (cxiSource2, sqlDate(dateRaw2), runId3, "ov_1", "hv_1", IdentityType.Email.code), // excluded by run id and date
                (cxiSource2, sqlDate(dateRaw2), runId1, "ov_1", "hv_1", IdentityType.Email.code), // excluded by date
            ))
            .toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")

        df.createOrReplaceTempView(tableName)

        // when
        val actual = PrivacyLookupDeduplicationJob.readPrivacyLookupIntermediateTable(tableName, feedDateToRunIdChangedDataSet)(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_source", "identity_type", "hashed_value", "original_value", "feed_date")
        }
        withClue("Actual read data frame data do not match") {
            val expected = spark.createDataFrame(
                List(
                    (cxiSource, IdentityType.Email.code, "hv_1", "ov_1", sqlDate(dateRaw)),
                    (cxiSource, IdentityType.Phone.code, "hv_2", "ov_2", sqlDate(dateRaw)),
                ))
                .toDF("cxi_source", "identity_type", "hashed_value", "original_value", "feed_date")
                .collect()
            actual.collect() should contain theSameElementsAs expected
        }

    }

    test("test transform") {
        // given
        val cxiSource = "cxi-usa-goldbowl"
        val cxiSource2 = "cxi-usa-burgerking"
        val dateRaw = "2022-02-24"
        val dateRaw2 = "2022-02-25"
        val runId1 = "runId1"
        val runId2 = "runId2"
        val df = spark.createDataFrame(
            List(
                (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId2, "ov_1", "hv_1", IdentityType.Email.code), // diff run id
                (cxiSource2, sqlDate(dateRaw), runId1, "ov_2", "hv_2", IdentityType.Phone.code), // diff cxi_source
                (cxiSource, sqlDate(dateRaw2), runId1, "ov_3", "hv_3", IdentityType.Email.code), // diff date
                (cxiSource, sqlDate(dateRaw), runId1, "ov_3", "hv_3", IdentityType.Phone.code) // diff identity type
            ))
            .toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")

        // when
        val actual = PrivacyLookupDeduplicationJob.transform(df)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_source", "identity_type", "hashed_value", "original_value", "feed_date")
        }
        import spark.implicits._
        withClue("Actual transformed data frame data do not match") {
            val expected = List(
                (cxiSource, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource2, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code), // diff cxi_source
                (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Phone.code) // diff identity type
            )
                .toDF("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
                .select("cxi_source", "identity_type", "hashed_value", "original_value", "feed_date") // just reorder
                .collect()
            actual.collect() should contain theSameElementsAs expected
        }
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}

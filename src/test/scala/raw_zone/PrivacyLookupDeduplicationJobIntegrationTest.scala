package com.cxi.cdp.data_processing
package raw_zone

import com.cxi.cdp.data_processing.raw_zone.PrivacyLookupDeduplicationJob.FeedDateToRunIdChangedData
import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityType
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class PrivacyLookupDeduplicationJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {
    import spark.implicits._

    val destDb = "default"
    val destTable = generateUniqueTableName("lookup_table_test")

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(s"$destDb.$destTable")
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(s"$destDb.$destTable")
    }

    test("test process") {
        // given
        val dateRaw = "2022-02-24"
        val dateRaw2 = "2022-02-25"
        val dateRaw3 = "2022-02-26" // excluded
        val runId1 = "runId1"
        val runId2 = "runId2"
        val runId3 = "runId3"
        val runId4 = "runId4" // excluded
        val runId5 = "runId5" // excluded

        val feedDateToRunIdPairsToProcess = Set(
            FeedDateToRunIdChangedData(dateRaw, runId1),
            FeedDateToRunIdChangedData(dateRaw, runId2),
            FeedDateToRunIdChangedData(dateRaw2, runId3)
        )
        val privacyIntermediateTableName = "tempPrivacyLookupIntermediateTable"
        val cxiSource = "cxi-usa-goldbowl"
        val cxiSource2 = "cxi-usa-burgerking"
        val df = spark
            .createDataFrame(
                List(
                    (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                    (
                        cxiSource2,
                        sqlDate(dateRaw),
                        runId1,
                        "ov_1",
                        "hv_1",
                        IdentityType.Email.code
                    ), // duplicate: diff cxi source, included
                    (
                        cxiSource,
                        sqlDate(dateRaw),
                        runId2,
                        "ov_1",
                        "hv_1",
                        IdentityType.Email.code
                    ), // duplicate: diff run id, excluded
                    (cxiSource, sqlDate(dateRaw), runId2, "ov_2", "hv_2", IdentityType.Phone.code),
                    (
                        cxiSource2,
                        sqlDate(dateRaw),
                        runId4,
                        "ov_3",
                        "hv_3",
                        IdentityType.Email.code
                    ), // excluded by run id
                    (
                        cxiSource2,
                        sqlDate(dateRaw3),
                        runId5,
                        "ov_1",
                        "hv_1",
                        IdentityType.Email.code
                    ) // excluded by run id and date
                )
            )
            .toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")

        df.createOrReplaceTempView(privacyIntermediateTableName)

        // when
        PrivacyLookupDeduplicationJob.process(
            privacyIntermediateTableName,
            s"$destDb.$destTable",
            feedDateToRunIdPairsToProcess
        )(spark)

        // then
        withClue("Saved pii data does not match") {
            val actual = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
            val expected = Seq(
                (cxiSource, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource2, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code)
            ).toDF("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("test writePrivacyLookup") {
        // given
        val cxiSource = "cxi-usa-goldbowl"
        val cxiSource2 = "cxi-usa-burgerking"
        val dateRaw = "2022-02-24"
        val df = List(
            (cxiSource, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
            (cxiSource, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code),
            (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Email.code),
            (cxiSource2, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code), // diff cxi_source
            (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Phone.code) // diff identity type
        ).toDF("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")

        // when
        PrivacyLookupDeduplicationJob.writePrivacyLookup(df, s"$destDb.$destTable")

        // then
        withClue("Saved pii data do not match") {
            val actual_1 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
            val expected_1 = Seq(
                (cxiSource, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource2, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code), // diff cxi_source
                (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Phone.code) // diff identity type
            ).toDF("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
            actual_1.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual_1.collect() should contain theSameElementsAs expected_1.collect()
        }

        // when
        PrivacyLookupDeduplicationJob.writePrivacyLookup(
            df,
            s"$destDb.$destTable"
        ) // same duplicate insert, dest table should stay w/o changes

        // then
        withClue("Saved pii data do not match") {
            val actual_2 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
            val expected_2 = Seq(
                (cxiSource, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource2, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code), // diff cxi_source
                (cxiSource, sqlDate(dateRaw), "ov_3", "hv_3", IdentityType.Phone.code) // diff identity type
            ).toDF("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
            actual_2.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual_2.collect() should contain theSameElementsAs expected_2.collect()
        }
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }

    def createTempTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE IF NOT EXISTS $tableName (
               |  `cxi_source` STRING,
               |  `identity_type` STRING,
               |  `original_value` STRING,
               |  `hashed_value` STRING,
               |  `feed_date` DATE
               |) USING delta
               |PARTITIONED BY (cxi_source, identity_type);
               |""".stripMargin)
    }
    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

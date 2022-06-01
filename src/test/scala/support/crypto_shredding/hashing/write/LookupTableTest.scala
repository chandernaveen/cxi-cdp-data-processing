package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.write

import refined_zone.hub.identity.model.IdentityType
import support.crypto_shredding.hashing.function_types.CryptoHashingResult
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class LookupTableTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {
    import spark.implicits._

    val destDb = "default"
    val destTable = generateUniqueTableName("lookup_table_intermediate_test")

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(s"$destDb.$destTable")
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(s"$destDb.$destTable")
    }

    test("test upsert to privacy") {
        // given
        val cxiSource = "cxi-usa-goldbowl"
        val dateRaw = "2022-02-24"
        val runId1 = "runId1"
        val lookupTable = new LookupTable(spark, destDb, destTable)

        // initial set of pii data
        val df = spark.createDataset(
            Seq(
                CryptoHashingResult("ov_1", "hv_1", IdentityType.Email.code),
                CryptoHashingResult("ov_2", "hv_2", IdentityType.Phone.code),
                CryptoHashingResult("ov_3", "hv_3", IdentityType.Email.code)
            )
        )

        // when
        lookupTable.upsert(df, cxiSource, dateRaw, runId1)

        // then
        withClue("Saved pii data does not match") {
            val actual_1 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            val expected_1 = Seq(
                (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_3", "hv_3", IdentityType.Email.code)
            ).toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            actual_1.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual_1.collect() should contain theSameElementsAs expected_1.collect()
        }

        // given
        // same set of pii data (emulate rerun), do not expect any new rows added
        val runId2 = runId1
        val df2 = spark.createDataset(
            Seq(
                CryptoHashingResult("ov_1", "hv_1", IdentityType.Email.code),
                CryptoHashingResult("ov_2", "hv_2", IdentityType.Phone.code),
                CryptoHashingResult("ov_3", "hv_3", IdentityType.Email.code)
            )
        )

        // when
        lookupTable.upsert(df2, cxiSource, dateRaw, runId2)

        // then
        withClue("Saved pii data does not match") {
            val actual_2 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            val expected_2 = Seq(
                (cxiSource, sqlDate(dateRaw), runId2, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId2, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId2, "ov_3", "hv_3", IdentityType.Email.code)
            ).toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            actual_2.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual_2.collect() should contain theSameElementsAs expected_2.collect()
        }

        // given
        // new pii data, expect new rows added
        val runId3 = "runId3"
        val df3 = spark.createDataset(
            Seq(
                CryptoHashingResult("ov_11", "hv_11", IdentityType.Email.code),
                CryptoHashingResult("ov_22", "hv_22", IdentityType.Phone.code),
                CryptoHashingResult("ov_33", "hv_33", IdentityType.Email.code)
            )
        )

        // when
        lookupTable.upsert(df3, cxiSource, dateRaw, runId3)

        // then
        withClue("Saved pii data does not match") {
            val actual_3 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            val expected_3 = Seq(
                (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_11", "hv_11", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_22", "hv_22", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_33", "hv_33", IdentityType.Email.code)
            ).toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            actual_3.schema.fields.map(_.name) shouldEqual expected_3.schema.fields.map(_.name)
            actual_3.collect() should contain theSameElementsAs expected_3.collect()
        }

        // given
        // new pii data (different feed_date)
        val runId4 = runId1
        val dateRaw4 = "2022-02-25"
        val df4 = spark.createDataset(
            Seq(
                CryptoHashingResult("ov_111", "hv_111", IdentityType.Email.code)
            )
        )

        // when
        lookupTable.upsert(df4, cxiSource, dateRaw4, runId4)

        // then
        withClue("Saved pii data does not match") {
            val actual_4 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            val expected_4 = Seq(
                (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_11", "hv_11", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_22", "hv_22", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_33", "hv_33", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw4), runId4, "ov_111", "hv_111", IdentityType.Email.code)
            ).toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            actual_4.schema.fields.map(_.name) shouldEqual expected_4.schema.fields.map(_.name)
            actual_4.collect() should contain theSameElementsAs expected_4.collect()
        }

        // given
        // new pii data (different cxi_source)
        val runId5 = runId4
        val dateRaw5 = "2022-02-25"
        val cxiSource5 = "cxi-usa-burgerking"
        val df5 = spark.createDataset(
            Seq(
                CryptoHashingResult(
                    "ov_111",
                    "hv_111",
                    IdentityType.Email.code
                ) // same pii previously inserted, expect copy
            )
        )

        // when
        lookupTable.upsert(df5, cxiSource5, dateRaw5, runId5)

        // then
        withClue("Saved pii data does not match") {
            val actual_4 = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            val expected_4 = Seq(
                (cxiSource, sqlDate(dateRaw), runId1, "ov_1", "hv_1", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_2", "hv_2", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId1, "ov_3", "hv_3", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_11", "hv_11", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_22", "hv_22", IdentityType.Phone.code),
                (cxiSource, sqlDate(dateRaw), runId3, "ov_33", "hv_33", IdentityType.Email.code),
                (cxiSource, sqlDate(dateRaw4), runId4, "ov_111", "hv_111", IdentityType.Email.code),
                (cxiSource5, sqlDate(dateRaw5), runId5, "ov_111", "hv_111", IdentityType.Email.code)
            ).toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")
            actual_4.schema.fields.map(_.name) shouldEqual expected_4.schema.fields.map(_.name)
            actual_4.collect() should contain theSameElementsAs expected_4.collect()
        }
    }

    def createTempTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE IF NOT EXISTS $tableName
               (
               |  `cxi_source` STRING,
               |  `identity_type` STRING,
               |  `original_value` STRING,
               |  `hashed_value` STRING,
               |  `feed_date` DATE,
               |  `run_id` STRING
               |) USING delta
               |PARTITIONED BY (feed_date, run_id, cxi_source, identity_type);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}

package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import refined_zone.hub.identity.model.IdentityType.CombinationCardSlim
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import support.normalization.DateNormalization.{formatFromLocalDate, parseToLocalDate}
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class PosIdentityProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_pos_identity_intermediate_omnivore")

    test("Write POS identity to intermediate table") {
        import spark.implicits._

        // given
        val feedDate = parseToLocalDate("2022-02-24")
        val feedDateString = formatFromLocalDate(feedDate).get
        val runId_1 = "run-Id-1"
        val identities_1 = Seq(
            ("hash001", CombinationCardSlim.value, "1", Map.empty[String, String])
        ).toDF(CxiIdentityId, Type, Weight, Metadata)

        // when
        // write identities_1 first time
        PosIdentityProcessor.writePosIdentities(identities_1, destTable, feedDateString, runId_1)

        // then
        val expected_1 = identities_1
            .withColumn("feed_date", lit(feedDate))
            .withColumn("run_id", lit(runId_1))

        withClue("Saved identities do not match") {
            val actual = spark.table(destTable)
            actual.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected_1.collect()
        }

        // when
        // write identities_1 one more time
        PosIdentityProcessor.writePosIdentities(identities_1, destTable, feedDateString, runId_1)

        // then
        // no duplicates
        withClue("Saved identities do not match") {
            val actual = spark.table(destTable)
            actual.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected_1.collect()
        }

        // given
        val identities_2 = Seq(
            ("hash002", CombinationCardSlim.value, "2", Map.empty[String, String])
        ).toDF(CxiIdentityId, Type, Weight, Metadata)

        // when
        // write identities_2 to the same partition
        PosIdentityProcessor.writePosIdentities(identities_2, destTable, feedDateString, runId_1)

        // then
        // partition overwritten
        val expected_2 = identities_2
            .withColumn("feed_date", lit(feedDate))
            .withColumn("run_id", lit(runId_1))

        withClue("Saved identities do not match") {
            val actual = spark.table(destTable)
            actual.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected_2.collect()
        }

        // given
        val runId_2 = "run-id-2" // new partition
        val identities_3 = Seq(
            ("hash003", CombinationCardSlim.value, "2", Map.empty[String, String])
        ).toDF(CxiIdentityId, Type, Weight, Metadata)

        // when
        // write identities_3 to the "runId_2" partition
        PosIdentityProcessor.writePosIdentities(identities_3, destTable, feedDateString, runId_2)

        // then
        // partition added
        val expected_3 = identities_3
            .withColumn("feed_date", lit(feedDate))
            .withColumn("run_id", lit(runId_2))
            .unionByName(expected_2)

        withClue("Saved identities do not match") {
            val actual = spark.table(destTable)
            actual.schema.fields.map(_.name) shouldEqual expected_3.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected_3.collect()
        }
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(destTable)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(destTable)
    }

    def createTempTable(tableName: String): Unit = {
        spark.sql(s"""
               CREATE TABLE IF NOT EXISTS $tableName
               (
                    `cxi_identity_id` STRING NOT NULL,
                    `type`            STRING NOT NULL,
                    `weight`          STRING NOT NULL,
                    `metadata`        MAP<STRING, STRING>,
                    `feed_date`       DATE NOT NULL,
                    `run_id`          STRING NOT NULL
               ) USING delta
               PARTITIONED BY (feed_date, run_id);
               """.stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

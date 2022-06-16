package com.cxi.cdp.data_processing
package refined_zone.hub.identity

import refined_zone.hub.identity.model.IdentityType.{CombinationBin, Email, IPv4, Phone}
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class PosIdentityDeduplicationJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    import spark.implicits._
    val destTable = generateUniqueTableName("integration_test_identity_table")

    test("Write final DF to the destination table") {

        // given
        val deletedIdentities_1 = Seq.empty[(String, String)].toDF("cxi_identity_id", "type")

        val newOrUpdatedIdentities_1 = Seq(
            ("hash_1", Email.value, "1", Map("email_domain" -> "mailbox.com")),
            ("hash_2", Phone.value, "2", null),
            ("hash_3", CombinationBin.value, "3", null)
        ).toDF("cxi_identity_id", "type", "weight", "metadata")

        // when
        // insert new data to empty table
        PosIdentityDeduplicationJob.writeIdentities(deletedIdentities_1, newOrUpdatedIdentities_1, destTable)

        // then
        withClue("Saved identities do not match") {
            val actual = spark.table(destTable)
            actual.schema.fields.map(_.name) shouldEqual newOrUpdatedIdentities_1.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs newOrUpdatedIdentities_1.collect()
        }

        // given
        val deletedIdentities_2 = Seq(
            ("hash_2", Phone.value)
        ).toDF("cxi_identity_id", "type")

        val newOrUpdatedIdentities_2 = Seq(
            ("hash_1", Email.value, "1", Map("email_domain" -> "google.com")), // updated
            ("hash_4", IPv4.value, "4", null) // new
        ).toDF("cxi_identity_id", "type", "weight", "metadata")

        // when
        // 1 deleted, 1 updated, 1 added
        PosIdentityDeduplicationJob.writeIdentities(deletedIdentities_2, newOrUpdatedIdentities_2, destTable)

        // then
        withClue("Saved identities do not match") {
            val expected = Seq(
                ("hash_1", Email.value, "1", Map("email_domain" -> "google.com")),
                ("hash_3", CombinationBin.value, "3", null),
                ("hash_4", IPv4.value, "4", null)
            ).toDF("cxi_identity_id", "type", "weight", "metadata")
            val actual = spark.table(destTable)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
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
               |CREATE TABLE IF NOT EXISTS $tableName
               |(
               |  `cxi_identity_id` STRING,
               |  `type` STRING,
               |  `weight` STRING,
               |  `metadata` MAP<STRING, STRING>
               |) USING delta;
               """.stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

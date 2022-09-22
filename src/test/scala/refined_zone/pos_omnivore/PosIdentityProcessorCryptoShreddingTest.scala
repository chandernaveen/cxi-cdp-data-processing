package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import support.crypto_shredding.config.CryptoShreddingConfig
import support.normalization.DateNormalization.parseToLocalDate
import support.normalization.TimestampNormalization.parseToTimestamp
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class PosIdentityProcessorCryptoShreddingTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destDb = "default"
    val destTable = generateUniqueTableName("test_lookup_table_intermediate_omnivore")

    test("Write to lookup table intermediate for CryptoShredding ") {
        import spark.implicits._

        // given
        val cryptoShreddingConfig = CryptoShreddingConfig(
            cxiSource = "partnerId_1",
            lookupDestDbName = destDb,
            lookupDestTableName = destTable,
            workspaceConfigPath = "dbfs:/databricks/config/workspace_details.json",
            date = parseToLocalDate("2022-08-24"),
            runId = "run-id"
        )

        val transformedOrderCustomersData = List(
            (
                "ordId_1",
                "partnerId_1",
                parseToTimestamp("2022-06-29T09:18:09Z"),
                "locId_1",
                "VISA",
                1234,
                ""
            )
        ).toDF(
            "ord_id",
            "cxi_partner_id",
            "ord_timestamp",
            "location_id",
            "tender_type",
            "ord_payment_last4",
            "ord_customer_full_name"
        )

        // when
        PosIdentityProcessor.computePosIdentitiesWeight2(cryptoShreddingConfig, transformedOrderCustomersData)(spark)

        // then
        withClue("Saved identities do not match") {
            val actual = spark
                .table(s"$destDb.$destTable")
                .select("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")

            val hashValue = actual.select("hashed_value").collect().map(_.getString(0)).mkString("")

            val expected = Seq(
                (
                    "partnerId_1",
                    sqlDate("2022-08-24"),
                    "run-id",
                    "-visa-1234",
                    hashValue,
                    IdentityType.CombinationCardSlim.code
                )
            ).toDF("cxi_source", "feed_date", "run_id", "original_value", "hashed_value", "identity_type")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(s"$destDb.$destTable")
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(s"$destDb.$destTable")
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

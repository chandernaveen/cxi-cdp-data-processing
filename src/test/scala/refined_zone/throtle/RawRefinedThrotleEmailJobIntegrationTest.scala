package com.cxi.cdp.data_processing
package refined_zone.throtle
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class RawRefinedThrotleEmailJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("tid_email_test")

    test("Write final DF to the destination table") {

        // given
        val rawStruct = new StructType()
            .add("throtle_id", StringType)
            .add("throtle_hhid", StringType)
            .add("sha256_lower_emails", DataTypes.createArrayType(StringType, false), false)

        val tid_emails_final = Seq(
            Row("throtle_id_1", "throtle_hhid_1", Array("email_hash_11")),
            Row("throtle_id_2", "throtle_hhid_2", Array("email_hash_21", "email_hash_22")),
            Row("throtle_id_3", null, Array("email_hash_31"))
        )

        import collection.JavaConverters._
        val tid_emails_final_df = spark.createDataFrame(tid_emails_final.asJava, rawStruct)

        // when
        RawRefinedThrotleEmailJob.writeThrotleEmails(tid_emails_final_df, destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)

            val expected = tid_emails_final_df

            import collection.JavaConverters._
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        RawRefinedThrotleEmailJob.writeThrotleEmails(tid_emails_final_df, destTable)

        // then
        withClue("Rewriting initial set of rows - emulate job re-run") {
            val actual = spark
                .table(destTable)

            val expected = tid_emails_final_df

            import collection.JavaConverters._
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        // Modify existing row, add new row
        val tid_emails_final_2 = Seq(
            Row("throtle_id_2", "throtle_hhid_2", Array("email_hash_21", "email_hash_23")),
            Row("throtle_id_4", "throtle_hhid_4", Array("email_hash_41"))
        )

        import collection.JavaConverters._
        val tid_emails_final_df_2 = spark.createDataFrame(tid_emails_final_2.asJava, rawStruct)

        // when
        RawRefinedThrotleEmailJob.writeThrotleEmails(tid_emails_final_df_2, destTable)

        // then
        withClue("Rewriting existing row - emails should overwrite with new") {
            val actual = spark
                .table(destTable)

            val expected_data = Seq(
                Row("throtle_id_1", "throtle_hhid_1", Array("email_hash_11")),
                Row("throtle_id_2", "throtle_hhid_2", Array("email_hash_21", "email_hash_23")),
                Row("throtle_id_3", null, Array("email_hash_31")),
                Row("throtle_id_4", "throtle_hhid_4", Array("email_hash_41"))
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expected_data.asJava, rawStruct)
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
                   |    `throtle_id`                string not null,
                   |    `throtle_hhid`              string,
                   |    `sha256_lower_emails`       array<STRING> NOT NULL
                   |) USING delta;
                   |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

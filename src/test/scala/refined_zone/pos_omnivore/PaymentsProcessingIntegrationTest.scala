package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table to  execute it locally")
class PaymentsProcessingIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_omnivore_payments")

    test("Write final DF to the destination table") {

        // given
        val payment_struct = new StructType()
            .add("cxi_partner_id", StringType)
            .add("payment_id", StringType)
            .add("order_id", StringType)
            .add("location_id", StringType)
            .add("status", StringType)
            .add("name", StringType)
            .add("card_brand", StringType)
            .add("pan", StringType)
            .add("bin", StringType)
            .add("exp_month", StringType)
            .add("exp_year", StringType)

        val payment_data = Seq(
            Row("cxi-usa-customerxicafe", "pay_123", "ord_123", "cjxi", null, "payment1", null, null, null, null, null),
            Row("cxi-usa-customerxicafe", "pay_2", "ord_2", "cjxi", null, "payment2", null, null, null, null, null),
            Row("cxi-usa-customerxicafe", "pay_3", "ord_3", "cjxi", null, "payment3", null, null, null, null, null)
        )

        import collection.JavaConverters._
        val payment_data_df = spark.createDataFrame(payment_data.asJava, payment_struct)

        // when
        PaymentsProcessor.writePayments(payment_data_df, "cxi-usa-customerxicafe", destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)

            val expected = payment_data_df

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        PaymentsProcessor.writePayments(payment_data_df, "cxi-usa-customerxicafe", destTable)

        // then
        withClue("Rewriting initial set of rows - emulate job re-run/Duplicate checks") {
            val actual = spark
                .table(destTable)

            val expected = payment_data_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given

        val payment_data_2 = Seq(
            Row(
                "cxi-usa-customerxicafe",
                "pay_123",
                "ord_123",
                "cjxi",
                null,
                "payment_upd",
                null,
                null,
                null,
                null,
                null
            ),
            Row("cxi-usa-customerxicafe", "pay_2", "ord_2", "cjxi", null, "payment2", null, null, null, null, null)
        )
        val payment_data_df_2 = spark.createDataFrame(payment_data_2.asJava, payment_struct)

        // when
        PaymentsProcessor.writePayments(payment_data_df_2, "cxi-usa-customerxicafe", destTable)

        // then
        withClue("Rewriting existing row - duplicate checks/data update") {
            val actual = spark
                .table(destTable)

            val expected_data = Seq(
                Row(
                    "cxi-usa-customerxicafe",
                    "pay_123",
                    "ord_123",
                    "cjxi",
                    null,
                    "payment_upd",
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                Row("cxi-usa-customerxicafe", "pay_2", "ord_2", "cjxi", null, "payment2", null, null, null, null, null),
                Row("cxi-usa-customerxicafe", "pay_3", "ord_3", "cjxi", null, "payment3", null, null, null, null, null)
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expected_data.asJava, payment_struct)
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
                    (
                     |`cxi_partner_id` STRING,
                     |`payment_id` STRING,
                     |`order_id` STRING,
                     |`location_id` STRING,
                     |`status` STRING,
                     |`name` STRING,
                     |`card_brand` STRING,
                     |`pan` STRING,
                     |`bin` STRING,
                     |`exp_month` STRING,
                     |`exp_year` STRING)
                     |USING delta
                     |PARTITIONED BY (cxi_partner_id)
                     |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

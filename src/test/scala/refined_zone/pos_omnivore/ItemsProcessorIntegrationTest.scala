package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table to  execute it locally")
class ItemsProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_omnivore_items")

    test("Write final DF to the destination table") {

        // given

        val item_struct = new StructType()
            .add("item_id", StringType)
            .add("cxi_partner_id", StringType)
            .add("item_nm", StringType)
            .add("item_desc", StringType)
            .add("item_type", StringType)
            .add("category_array", StringType)
            .add("main_category_name", StringType)
            .add("variation_array", StringType)
            .add("item_plu", StringType)
            .add("item_barcode", StringType)

        val item_data = Seq(
            Row(
                "Ord_1",
                "cxi-usa-customerxicafe",
                "BURGER",
                null,
                "FOOD",
                "APPETIZER,FOOD",
                "FOOD",
                "SIDE,CHEESE",
                null,
                null
            ),
            Row(
                "Ord_2",
                "cxi-usa-customerxicafe",
                "PIZZA",
                null,
                "FOOD",
                "APPETIZER,FOOD",
                "FOOD",
                "SIDE,CHEESE",
                null,
                null
            )
        )

        import collection.JavaConverters._
        val item_data_df = spark.createDataFrame(item_data.asJava, item_struct)

        // when
        ItemsProcessor.writeItems(item_data_df, "cxi-usa-customerxicafe", destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)

            val expected = item_data_df

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        ItemsProcessor.writeItems(item_data_df, "cxi-usa-customerxicafe", destTable)

        // then
        withClue("Rewriting initial set of rows - emulate job re-run/Duplicate checks") {
            val actual = spark
                .table(destTable)

            val expected = item_data_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given

        val item_data_2 = Seq(
            Row(
                "Ord_2",
                "cxi-usa-customerxicafe",
                "APPLEJUICE",
                null,
                "DRINKS",
                "APPETIZER,DRINKS",
                "DRINKS",
                "SIDE,CHEESE",
                null,
                null
            ),
            Row(
                "Ord_3",
                "cxi-usa-customerxicafe",
                "PIZZA",
                null,
                "FOOD",
                "APPETIZER,FOOD",
                "FOOD",
                "SIDE,CHEESE",
                null,
                null
            )
        )
        val item_data_df_2 = spark.createDataFrame(item_data_2.asJava, item_struct)

        // when
        ItemsProcessor.writeItems(item_data_df_2, "cxi-usa-customerxicafe", destTable)

        // then
        withClue("Rewriting existing row - duplicate checks/data update") {
            val actual = spark
                .table(destTable)

            val expected_data = Seq(
                Row(
                    "Ord_2",
                    "cxi-usa-customerxicafe",
                    "APPLEJUICE",
                    null,
                    "DRINKS",
                    "APPETIZER,DRINKS",
                    "DRINKS",
                    "SIDE,CHEESE",
                    null,
                    null
                ),
                Row(
                    "Ord_3",
                    "cxi-usa-customerxicafe",
                    "PIZZA",
                    null,
                    "FOOD",
                    "APPETIZER,FOOD",
                    "FOOD",
                    "SIDE,CHEESE",
                    null,
                    null
                ),
                Row(
                    "Ord_1",
                    "cxi-usa-customerxicafe",
                    "BURGER",
                    null,
                    "FOOD",
                    "APPETIZER,FOOD",
                    "FOOD",
                    "SIDE,CHEESE",
                    null,
                    null
                )
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expected_data.asJava, item_struct)
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
                     |`item_id` STRING,
                     |`cxi_partner_id` STRING,
                     |`item_nm` STRING,
                     |`item_desc` STRING,
                     |`item_type` STRING,
                     |`category_array` STRING,
                     |`main_category_name` STRING,
                     |`variation_array` STRING,
                     |`item_plu` STRING,
                     |`item_barcode` STRING)
                     | USING delta
                     | PARTITIONED BY (cxi_partner_id)
                     |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

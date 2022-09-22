package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructField}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class CategoriesProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("CategoriesProcessorJob_test")

    test("Write final DF to the destination table") {

        val CategoriesSchema = DataTypes.createStructType(
            Array(
                StructField("location_id", StringType),
                StructField("cxi_partner_id", StringType),
                StructField("cat_desc", StringType),
                StructField("cat_id", StringType),
                StructField("cat_nm", StringType)
            )
        )

        val categories_final = Seq(
            Row("Tkgqojgc1", "cxi-usa-goldbowl", null, "234", "APPETIZERS"),
            Row("Tkgqojgc2", "cxi-usa-goldbowl", null, "345", "APPETIZERS"),
            Row("Tkgqojgc3", "cxi-usa-goldbowl", null, "231", "APPETIZERS")
        )

        import collection.JavaConverters._
        val categories_final_df = spark.createDataFrame(categories_final.asJava, CategoriesSchema)

        // when
        CategoriesProcessor.writeCategories(categories_final_df, "cxi-usa-goldbowl", destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)

            val expected = categories_final_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        CategoriesProcessor.writeCategories(categories_final_df, "cxi-usa-goldbowl", destTable)

        // then
        withClue("Rewriting initial set of rows - emulate job re-run") {
            val actual = spark
                .table(destTable)

            val expected = categories_final_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        // Modify existing row, add new row
        val categories_final_2 = Seq(
            Row("Tkgqojgc3", "cxi-usa-goldbowl", null, "231", "APPETIZERS1"),
            Row("Tkgqojgc4", "cxi-usa-goldbowl", null, "233", "APPETIZERS")
        )
        val categories_final_df_2 = spark.createDataFrame(categories_final_2.asJava, CategoriesSchema)

        // when
        CategoriesProcessor.writeCategories(categories_final_df_2, "cxi-usa-goldbowl", destTable)

        // then
        withClue("Rewriting existing row - location should overwrite with new") {
            val actual = spark
                .table(destTable)

            val expected_data = Seq(
                Row("Tkgqojgc1", "cxi-usa-goldbowl", null, "234", "APPETIZERS"),
                Row("Tkgqojgc2", "cxi-usa-goldbowl", null, "345", "APPETIZERS"),
                Row("Tkgqojgc3", "cxi-usa-goldbowl", null, "231", "APPETIZERS1"),
                Row("Tkgqojgc4", "cxi-usa-goldbowl", null, "233", "APPETIZERS")
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expected_data.asJava, CategoriesSchema)
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
                   |    `location_id`     string not null,
                   |    `cxi_partner_id`  string,
                   |    `cat_desc`        string,
                   |    `cat_id`          string,
                   |    `cat_nm`          string
                   |) USING delta;
                   |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructField}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class OrderTendersIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("OrderTendersJob_test")

    test("Write final DF to the destination table") {

        // given
        val CategoriesSchema = DataTypes.createStructType(
            Array(
                StructField("tender_id", StringType),
                StructField("cxi_partner_id", StringType),
                StructField("location_id", StringType),
                StructField("tender_nm", StringType),
                StructField("tender_type", StringType)
            )
        )
        val orderTenders_final = Seq(
            Row("123", "partner-1", "14771", "3rd Party", "91"),
            Row("678", "partner-1", "14772", "3rd Party", "92"),
            Row("980", "partner-1", "14773", "3rd Party", "93")
        )

        import collection.JavaConverters._
        val orderTenders_final_df = spark.createDataFrame(orderTenders_final.asJava, CategoriesSchema)

        // when
        OrderTendersProcessor.writeOrderTenders(orderTenders_final_df, "partner-1", destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)

            val expected = orderTenders_final_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        OrderTendersProcessor.writeOrderTenders(orderTenders_final_df, "partner-1", destTable)
        // then
        withClue("Rewriting initial set of rows - emulate job re-run") {
            val actual = spark
                .table(destTable)

            val expected = orderTenders_final_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        // Modify existing row, add new row
        val orderTenders_final_2 = Seq(
            Row("980", "partner-1", "14773", "3rd Party", "94"),
            Row("981", "partner-1", "14775", "3rd Party", "95")
        )
        val orderTenders_final_df_2 = spark.createDataFrame(orderTenders_final_2.asJava, CategoriesSchema)
        // when
        OrderTendersProcessor.writeOrderTenders(orderTenders_final_df_2, "partner-1", destTable)

        // then
        withClue("Rewriting existing row - location should overwrite with new") {
            val actual = spark
                .table(destTable)
            val expected_data = Seq(
                Row("123", "partner-1", "14771", "3rd Party", "91"),
                Row("678", "partner-1", "14772", "3rd Party", "92"),
                Row("980", "partner-1", "14773", "3rd Party", "94"),
                Row("981", "partner-1", "14775", "3rd Party", "95")
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
                   |    `tender_id`       string,
                   |    `cxi_partner_id`  string not null,
                   |    `location_id`     string,
                   |    `tender_nm`       string,
                   |    `tender_type`     string
                   |) USING delta;
                   |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

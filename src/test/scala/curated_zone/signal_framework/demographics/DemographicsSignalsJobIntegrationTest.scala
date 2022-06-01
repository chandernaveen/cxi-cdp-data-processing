package com.cxi.cdp.data_processing
package curated_zone.signal_framework.demographics

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import collection.JavaConverters._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class DemographicsSignalsJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    import spark.implicits._

    val destTable = generateUniqueTableName("integration_test_daily_generic_signals_table")

    test("test write") {
        // given
        val feedDate = "2022-02-24"
        val schema = StructType(
            Array(
                StructField("customer_360_id", StringType, true),
                StructField("signal_generation_date", StringType, false),
                StructField("signal_domain", StringType, false),
                StructField("signal_name", StringType, false),
                StructField("signal_value", StringType, false)
            )
        )
        val firstInsertOverwrite = spark.createDataFrame(
            Seq(
                Row("uuid1", feedDate, "profile", "age", "1"),
                Row("uuid2", feedDate, "profile", "age", "2")
            ).asJava,
            schema
        )

        // when
        DemographicsSignalsJob.write(firstInsertOverwrite, feedDate, "profile", "age", destTable)

        // then
        withClue("Saved signals do not match") {
            val actual_1 = spark
                .table(destTable)
                .select("customer_360_id", "signal_generation_date", "signal_domain", "signal_name", "signal_value")
            val expected_1 =
                firstInsertOverwrite.withColumn("signal_generation_date", col("signal_generation_date").cast(DateType))
            actual_1.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual_1.collect() should contain theSameElementsAs expected_1.collect()
        }

        // given, inserts for diff domain
        val secondInsertOverwrite = spark.createDataFrame(
            Seq(
                Row("uuid1", feedDate, "some_other_domain", "children", "true"),
                Row("uuid2", feedDate, "some_other_domain", "children", "false")
            ).asJava,
            schema
        )

        // when
        DemographicsSignalsJob.write(secondInsertOverwrite, feedDate, "some_other_domain", "children", destTable)

        // then
        withClue("Saved signals do not match") {
            val actual_2 = spark
                .table(destTable)
                .select("customer_360_id", "signal_generation_date", "signal_domain", "signal_name", "signal_value")
            val expected_2 = firstInsertOverwrite
                .unionByName(secondInsertOverwrite)
                .withColumn("signal_generation_date", col("signal_generation_date").cast(DateType))
            actual_2.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual_2.collect() should contain theSameElementsAs expected_2.collect()
        }

        // given, insert overwrites for same feed date + domain + signal name
        val thirdInsertOverwrite = spark.createDataFrame(
            Seq(
                Row("uuid1", feedDate, "some_other_domain", "children", "false"), // overwrite existing signal_value
                Row("uuid2", feedDate, "some_other_domain", "children", "true") // overwrite existing signal_value
            ).asJava,
            schema
        )

        // when
        DemographicsSignalsJob.write(thirdInsertOverwrite, feedDate, "some_other_domain", "children", destTable)

        // then
        withClue("Saved signals do not match") {
            val actual_3 = spark
                .table(destTable)
                .select("customer_360_id", "signal_generation_date", "signal_domain", "signal_name", "signal_value")
            val expected_3 = firstInsertOverwrite
                .unionByName(thirdInsertOverwrite)
                .withColumn("signal_generation_date", col("signal_generation_date").cast(DateType))
            actual_3.schema.fields.map(_.name) shouldEqual expected_3.schema.fields.map(_.name)
            actual_3.collect() should contain theSameElementsAs expected_3.collect()
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
               |    `customer_360_id`        string not null,
               |    `signal_domain`          string not null,
               |    `signal_name`            string not null,
               |    `signal_value`           string not null,
               |    `signal_generation_date` date   not null
               |) USING delta
               |PARTITIONED BY (signal_generation_date, signal_domain, signal_name);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}

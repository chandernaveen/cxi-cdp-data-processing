package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class LocationProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("LocationProcessorJob_test")

    test("Write final DF to the destination table") {

        // given
        val location_Struct = new StructType()
            .add("location_id", StringType)
            .add("cxi_partner_id", StringType)
            .add("zip_code", StringType)
            .add("location_nm", StringType)
            .add("location_website", StringType)
            .add("location_type", StringType)
            .add("address_1", StringType)
            .add("address_2", StringType)
            .add("lat", StringType)
            .add("long", StringType)
            .add("phone", StringType)
            .add("country_code", StringType)
            .add("timezone", StringType)
            .add("currency", StringType)
            .add("open_dt", StringType)
            .add("active_flg", StringType)
            .add("fax", StringType)
            .add("parent_location_id", StringType)
            .add("extended_attr", StringType)
            .add("city", StringType)
            .add("state_code", StringType)
            .add("region", StringType)

        val location_final = Seq(
            Row(
                "Tkgqojgc1",
                "cxi-usa-goldbowl",
                "10111",
                "MicrosTest",
                "website.com",
                "1",
                "STREET-1",
                "STREET-2",
                null,
                null,
                null,
                "2022-06-29",
                "partner-1",
                "1",
                null,
                null,
                null,
                null,
                null,
                "Atlanta",
                "10189",
                "North"
            ),
            Row(
                "Tkgqojgc2",
                "cxi-usa-goldbowl",
                "10112",
                "MicrosTest",
                "website.com",
                "1",
                "STREET-1",
                "STREET-2",
                null,
                null,
                null,
                "2022-06-29",
                "partner-1",
                "1",
                null,
                null,
                null,
                null,
                null,
                "Atlanta",
                "10189",
                "North"
            ),
            Row(
                "Tkgqojgc3",
                "cxi-usa-goldbowl",
                "10113",
                "MicrosTest",
                "website.com",
                "1",
                "STREET-1",
                "STREET-2",
                null,
                null,
                null,
                "2022-06-29",
                "partner-1",
                "1",
                null,
                null,
                null,
                null,
                null,
                "Atlanta",
                "10189",
                "North"
            )
        )

        import collection.JavaConverters._
        val location_final_df = spark.createDataFrame(location_final.asJava, location_Struct)
        // when
        LocationProcessor.writeLocation(location_final_df, "cxi-usa-goldbowl", destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)
            val expected = location_final_df

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        LocationProcessor.writeLocation(location_final_df, "cxi-usa-goldbowl", destTable)
        // then
        withClue("Rewriting initial set of rows - emulate job re-run") {
            val actual = spark
                .table(destTable)
            val expected = location_final_df
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        // Modify existing row, add new row
        val categories_final_2 = Seq(
            Row(
                "Tkgqojgc3",
                "cxi-usa-goldbowl",
                "10123",
                "MicrosTest",
                "website.com",
                "1",
                "STREET-1",
                "STREET-2",
                null,
                null,
                null,
                "2022-06-29",
                "partner-1",
                "1",
                null,
                null,
                null,
                null,
                null,
                "Atlanta",
                "10189",
                "North"
            ),
            Row(
                "Tkgqojgc4",
                "cxi-usa-goldbowl",
                "10114",
                "MicrosTest",
                "website.com",
                "1",
                "STREET-1",
                "STREET-2",
                null,
                null,
                null,
                "2022-06-29",
                "partner-1",
                "1",
                null,
                null,
                null,
                null,
                null,
                "Atlanta",
                "10189",
                "North"
            )
        )
        val location_final_df_2 = spark.createDataFrame(categories_final_2.asJava, location_Struct)
        // when
        LocationProcessor.writeLocation(location_final_df_2, "cxi-usa-goldbowl", destTable)
        // then
        withClue("Rewriting existing row - location should overwrite with new") {
            val actual = spark
                .table(destTable)

            val expected_data = Seq(
                Row(
                    "Tkgqojgc1",
                    "cxi-usa-goldbowl",
                    "10111",
                    "MicrosTest",
                    "website.com",
                    "1",
                    "STREET-1",
                    "STREET-2",
                    null,
                    null,
                    null,
                    "2022-06-29",
                    "partner-1",
                    "1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    "Atlanta",
                    "10189",
                    "North"
                ),
                Row(
                    "Tkgqojgc2",
                    "cxi-usa-goldbowl",
                    "10112",
                    "MicrosTest",
                    "website.com",
                    "1",
                    "STREET-1",
                    "STREET-2",
                    null,
                    null,
                    null,
                    "2022-06-29",
                    "partner-1",
                    "1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    "Atlanta",
                    "10189",
                    "North"
                ),
                Row(
                    "Tkgqojgc3",
                    "cxi-usa-goldbowl",
                    "10123",
                    "MicrosTest",
                    "website.com",
                    "1",
                    "STREET-1",
                    "STREET-2",
                    null,
                    null,
                    null,
                    "2022-06-29",
                    "partner-1",
                    "1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    "Atlanta",
                    "10189",
                    "North"
                ),
                Row(
                    "Tkgqojgc4",
                    "cxi-usa-goldbowl",
                    "10114",
                    "MicrosTest",
                    "website.com",
                    "1",
                    "STREET-1",
                    "STREET-2",
                    null,
                    null,
                    null,
                    "2022-06-29",
                    "partner-1",
                    "1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    "Atlanta",
                    "10189",
                    "North"
                )
            )
            import collection.JavaConverters._
            val expected = spark.createDataFrame(expected_data.asJava, location_Struct)
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
                     |    `location_id`        string,
                     |    `cxi_partner_id`     string,
                     |    `zip_code`           string,
                     |    `location_nm`        string,
                     |    `location_website`   string,
                     |    `location_type`      string,
                     |    `address_1`          string,
                     |    `address_2`          string,
                     |    `lat`                string,
                     |    `long`               string,
                     |    `phone`              string,
                     |    `country_code`       string,
                     |    `timezone`           string,
                     |    `currency`           string,
                     |    `open_dt`            string,
                     |    `active_flg`         string,
                     |    `fax`                string,
                     |    `parent_location_id` string,
                     |    `extended_attr`      string,
                     |    `city`               string,
                     |    `state_code`         string,
                     |    `region`             string
                     |) USING delta;
                     |""".stripMargin)
    }
    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}

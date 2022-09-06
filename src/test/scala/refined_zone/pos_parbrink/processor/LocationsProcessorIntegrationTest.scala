package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.processor.LocationsProcessor.{ActiveFlagDefault, CurrencyDefault, LocationTypeDefault}
import refined_zone.pos_parbrink.processor.LocationsProcessorTest.LocationRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class LocationsProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_locations_processor")

    test("write Parbrink locations") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-partner-id-1"

        val location_A = LocationRefined(
            location_id = "loc_id_1",
            cxi_partner_id = cxiPartnerId,
            location_type = LocationTypeDefault,
            location_nm = "Miguel's Jr. - Original",
            location_website = null,
            active_flg = ActiveFlagDefault,
            address_1 = "1039 West 6th St.",
            address_2 = null,
            city = "Corona",
            state_code = "CA",
            region = null,
            zip_code = "92882",
            lat = "33.877219",
            longitude = "-117.581545",
            phone = "19512703551",
            fax = null,
            country_code = "US",
            parent_location_id = null,
            currency = CurrencyDefault,
            open_dt = null,
            timezone = "Pacific",
            extended_attr = null
        )
        val locations_1 = Seq(
            location_A
        ).toDF()
            .withColumnRenamed("longitude", "long")

        // when
        // write locations_1 first time
        LocationsProcessor.writeLocations(locations_1, cxiPartnerId, destTable)

        // then
        withClue("Saved Locations do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", locations_1.columns.size, actual.columns.size)
            assertDataFrameEquals(locations_1, actual)
        }

        // when
        // write locations_1 one more time
        LocationsProcessor.writeLocations(locations_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved Locations do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", locations_1.columns.size, actual.columns.size)
            assertDataFrameEquals(locations_1, actual)
        }

        // given
        val location_A_modified = location_A.copy(location_nm = "Location A name updated")
        val location_B = LocationRefined(
            location_id = "loc_id_2",
            cxi_partner_id = cxiPartnerId,
            location_type = LocationTypeDefault,
            location_nm = "* Darrin Heisey",
            location_website = null,
            active_flg = ActiveFlagDefault,
            address_1 = "11545 W. Bernardo Ct",
            address_2 = "Suite 150",
            city = "Corona",
            state_code = "CA",
            region = "SouthWest",
            zip_code = "92881",
            lat = "33.025114",
            longitude = "-117.082445",
            phone = null,
            fax = "1234567890",
            country_code = "US",
            parent_location_id = null,
            currency = CurrencyDefault,
            open_dt = null,
            timezone = null,
            extended_attr = null
        )
        val locations_2 = Seq(location_A_modified, location_B)
            .toDF()
            .withColumnRenamed("longitude", "long")

        // when
        // write modified location_A and new location_B
        LocationsProcessor.writeLocations(locations_2, cxiPartnerId, destTable)

        // then
        // Location_A updated, Location_B added
        withClue("Saved Locations do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", locations_2.columns.size, actual.columns.size)
            assertDataFrameEquals(locations_2, actual)
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
               |    `location_id`        STRING,
               |    `cxi_partner_id`     STRING,
               |    `location_type`      STRING,
               |    `location_nm`        STRING,
               |    `location_website`   STRING,
               |    `active_flg`         STRING,
               |    `address_1`          STRING,
               |    `address_2`          STRING,
               |    `city`               STRING,
               |    `state_code`         STRING,
               |    `region`             STRING,
               |    `zip_code`           STRING,
               |    `lat`                STRING,
               |    `long`               STRING,
               |    `phone`              STRING,
               |    `fax`                STRING,
               |    `country_code`       STRING,
               |    `parent_location_id` STRING,
               |    `currency`           STRING,
               |    `open_dt`            TIMESTAMP,
               |    `timezone`           STRING,
               |    `extended_attr`      STRING
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}

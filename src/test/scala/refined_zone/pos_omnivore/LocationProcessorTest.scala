package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import support.normalization.TimestampNormalization.parseToTimestamp
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers

class LocationProcessorTest extends BaseSparkBatchJobTest with Matchers {

    test("Read Location Details") {

        import spark.implicits._
        val LocationFile =
            List(
                (
                    s"""
                   |{"_links":{},"address":{"street1":"STREET-1","street2":"STREET-2","zip":"10112","country":"USA"},"agent_version":"21.12.21","concept_name":"WhiteLabel","created":1489529380,
                   |"development":true,"display_name":"MicrosTest","health":{"agent":{"healthy":true},"healthy":true,"system":{"healthy":true},"tickets":{"status":"functional"}},"id":"cjgjjkpi","modified":1642512397,"name":"MicrosTest",
                   |"owner":"CARDFREE","pos_type":"micros3700","status":"ONLINE","phone":"12345","timezone":"UTC","currency":"USD",
                   |"updater_version":"21.12.21","created_at":"1656519489","website":"website.com","type":"1","coordinates":{"latitude":"1","longitude":"2"}}
                   |""".stripMargin,
                    "locations",
                    "2022-04-13"
                )
            ).toDF("record_value", "record_Type", "feed_date")

        val LocationTableName = "Location_Table"
        LocationFile.createOrReplaceTempView(LocationTableName)

        val actualRead = LocationProcessor.readLocations(spark, "2022-04-13", LocationTableName)

        val locationsRead = List(
            (
                "cjgjjkpi", // id
                "MicrosTest", // display_name
                "MicrosTest", // name
                "website.com",
                "1",
                "ONLINE",
                "STREET-1",
                "STREET-2",
                "10112",
                "1",
                "2",
                "12345",
                "USA",
                "UTC",
                "USD",
                "1656519489"
            )
        ).toDF(
            "location_id",
            "location_nm",
            "location_nm1",
            "location_website",
            "location_type",
            "status",
            "address_1",
            "address_2",
            "zip_code",
            "lat",
            "long",
            "phone",
            "country_code",
            "timezone",
            "currency",
            "open_dt"
        ).collect()

        val actualOmnivoreLocationsReadData = actualRead.collect()
        val actualFieldsReturned = actualRead.schema.fields.map(f => f.name)

        withClue("Actual fields returned:\n" + actualRead.schema.treeString) {

            actualFieldsReturned shouldEqual Array(
                "location_id",
                "location_nm",
                "location_nm1",
                "location_website",
                "location_type",
                "status",
                "address_1",
                "address_2",
                "zip_code",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt"
            )
        }

        withClue("POS Omnivore raw- locations data do not match") {
            actualOmnivoreLocationsReadData.length should equal(locationsRead.length)
            actualOmnivoreLocationsReadData should contain theSameElementsAs locationsRead
        }

    }

    test("transform Location Details") {

        import spark.implicits._
        val locations = List(
            (
                "cjgjjkpi", // id
                "MicrosTest", // display_name
                "MicrosTest", // name
                "website.com",
                1,
                "ONLINE",
                "STREET-1",
                "STREET-2",
                "10112",
                null,
                null,
                null,
                "USA",
                null,
                null,
                1656519489
            )
        ).toDF(
            "location_id",
            "location_nm",
            "location_nm1",
            "location_website",
            "location_type",
            "status",
            "address_1",
            "address_2",
            "zip_code",
            "lat",
            "long",
            "phone",
            "country_code",
            "timezone",
            "currency",
            "open_dt"
        )

        // given

        val postalCd =
            Seq(
                (
                    "10112", // id
                    "Atlanta", // display_name
                    "10189",
                    "North"
                )
            ).toDF(
                "zip_code",
                "city",
                "state_code",
                "region"
            )

        // when
        val actual =
            LocationProcessor.transformLocations(locations, postalCd, "partner-1")

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "zip_code",
                "location_id",
                "location_nm",
                "location_website",
                "location_type",
                "address_1",
                "address_2",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt",
                "cxi_partner_id",
                "active_flg",
                "fax",
                "parent_location_id",
                "extended_attr",
                "city",
                "state_code",
                "region"
            )
        }

        val actualOmnivoreLocationsData = actual.collect()
        withClue("POS Omnivore  refined locations data do not match") {
            val expected = List(
                (
                    "10112",
                    "cjgjjkpi",
                    "MicrosTest",
                    "website.com",
                    "1",
                    "STREET-1",
                    "STREET-2",
                    null,
                    null,
                    null,
                    "USA",
                    "UTC",
                    "USD",
                    parseToTimestamp("2022-06-29T16:18:09Z"),
                    "partner-1",
                    "1",
                    null,
                    null,
                    null,
                    "Atlanta",
                    "10189",
                    "North"
                )
            ).toDF(
                "zip_code",
                "location_id",
                "location_nm",
                "location_website",
                "location_type",
                "address_1",
                "address_2",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt",
                "cxi_partner_id",
                "active_flg",
                "fax",
                "parent_location_id",
                "extended_attr",
                "city",
                "state_code",
                "region"
            ).collect()
            actualOmnivoreLocationsData.length should equal(expected.length)
            actualOmnivoreLocationsData should contain theSameElementsAs expected
        }

    }

}

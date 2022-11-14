package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.hub.model.LocationType
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

import java.sql.Timestamp.from
import java.time.LocalDateTime.of
import java.time.ZoneOffset.UTC

class LocationsProcessorTest extends BaseSparkBatchJobTest {

    test("test square partner location read") {
        // given
        import spark.implicits._
        val locations = List(
            (
                s"""
                   {
                       "id":"L0P0DJ340FXF0",
                       "name":"#8 Desert Ridge",
                       "type":"PHYSICAL",
                       "status":"ACTIVE",
                       "address":{
                          "address_line_1":"21001 N Tatum Blvd Suite 34-1130",
                          "postal_code":"85050",
                          "country":"US"
                       },
                       "coordinates":{
                          "latitude":"33.677158",
                          "longitude":"-111.977239"
                       },
                       "phone_number":"df2e49446821163cbf58168c436fec5fb2b09b4b12496fdc4e38d3e9784c2790",
                       "timezone":"America/Phoenix",
                       "currency":"USD",
                       "created_at":"2021-05-13T21:50:55Z",
                       "website_url":"www.cupbop.com"
                    }
                   """,
                "locations",
                "2021-10-11"
            ),
            (
                s"""
                   {
                     "id": "L0P0DJ340FXF0"
                   }
                   """,
                "locations",
                "2021-10-10"
            ) // duplicate with diff date that gets filtered out
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "locations"
        locations.createOrReplaceTempView(tableName)

        // when
        val actual = LocationsProcessor.readLocations(spark, "2021-10-11", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "location_id",
                "location_nm",
                "location_type",
                "active_flg",
                "address_1",
                "zip_code",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt",
                "location_website"
            )
        }
        val actualSquareLocationsData = actual.collect()
        withClue("POS Square refined locations data do not match") {
            val expected = List(
                (
                    "L0P0DJ340FXF0",
                    "#8 Desert Ridge",
                    "PHYSICAL",
                    "ACTIVE",
                    "21001 N Tatum Blvd Suite 34-1130",
                    "85050",
                    "33.677158",
                    "-111.977239",
                    "df2e49446821163cbf58168c436fec5fb2b09b4b12496fdc4e38d3e9784c2790",
                    "US",
                    "America/Phoenix",
                    "USD",
                    "2021-05-13T21:50:55Z",
                    "www.cupbop.com"
                )
            ).toDF(
                "location_id",
                "location_nm",
                "location_type",
                "active_flg",
                "address_1",
                "zip_code",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt",
                "location_website"
            ).collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected
        }
    }

    test("test square partner location transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val locations = List(
            (
                "L0P0DJ340FXF0",
                "PHYSICAL",
                "ACTIVE",
                null,
                null,
                "98765-4321",
                null,
                null,
                null,
                null,
                "America/Phoenix",
                null,
                "2021-05-13T21:50:55.123Z",
                null
            ),
            (
                "L0P0DJ340FXF0",
                "PHYSICAL",
                "ACTIVE",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "America/Phoenix",
                null,
                "2021-05-13T21:50:55.123Z",
                null
            ), // duplicate
            (
                "ACA0DJ910FPO1",
                "MOBILE",
                "INACTIVE",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "America/Phoenix",
                null,
                "2022-02-24T04:30:00.000Z",
                null
            ),
            (
                "PQW0DJ340ALS0",
                "some_other_loc_type",
                "ACTIVE",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "America/Phoenix",
                null,
                null,
                null
            )
        ).toDF(
            "location_id",
            "location_type",
            "active_flg",
            "location_nm",
            "address_1",
            "zip_code",
            "lat",
            "long",
            "phone",
            "country_code",
            "timezone",
            "currency",
            "open_dt",
            "location_website"
        )

        val postalCodes = Seq(
            ("98765", "US", "NY", "Central")
        ).toDF("zip_code", "state", "city", "region")

        // when
        val actual = LocationsProcessor.transformLocations(locations, postalCodes, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "zip_code",
                "location_id",
                "location_type",
                "active_flg",
                "location_nm",
                "address_1",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt",
                "location_website",
                "cxi_partner_id",
                "address_2",
                "fax",
                "parent_location_id",
                "extended_attr",
                "state",
                "city",
                "region"
            )
        }
        val actualSquareLocationsData = actual.collect()
        withClue("POS Square refined locations data do not match") {
            val expected = List(
                (
                    "98765",
                    "L0P0DJ340FXF0",
                    LocationType.Restaurant.code,
                    1,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "America/Phoenix",
                    null,
                    from(of(2021, 5, 13, 21, 50, 55, 123000000).toInstant(UTC)),
                    null,
                    cxiPartnerId,
                    null,
                    null,
                    null,
                    null,
                    "US",
                    "NY",
                    "Central"
                ),
                (
                    null,
                    "ACA0DJ910FPO1",
                    LocationType.Mobile.code,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "America/Phoenix",
                    null,
                    from(of(2022, 2, 24, 4, 30, 0, 0).toInstant(UTC)),
                    null,
                    cxiPartnerId,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                (
                    null,
                    "PQW0DJ340ALS0",
                    LocationType.Other.code,
                    1,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "America/Phoenix",
                    null,
                    null,
                    null,
                    cxiPartnerId,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            ).toDF(
                "zip_code",
                "location_id",
                "location_type",
                "active_flg",
                "location_nm",
                "address_1",
                "lat",
                "long",
                "phone",
                "country_code",
                "timezone",
                "currency",
                "open_dt",
                "location_website",
                "cxi_partner_id",
                "address_2",
                "city",
                "state",
                "region",
                "fax",
                "parent_location_id",
                "extended_attr"
            ).collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected
        }
    }

}

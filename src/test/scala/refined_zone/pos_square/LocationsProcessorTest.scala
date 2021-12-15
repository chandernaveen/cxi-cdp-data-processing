package com.cxi.cdp.data_processing
package refined_zone.pos_square

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

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
                   """, "locations" , "2021-10-11"),
            (
                s"""
                   {
                     "id": "L0P0DJ340FXF0"
                   }
                   """, "locations" , "2021-10-10") // duplicate with diff date that gets filtered out
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "locations"
        locations.createOrReplaceTempView(tableName)

        // when
        val actual = LocationsProcessor.readLocations(spark, "2021-10-11", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("location_id", "location_nm", "location_type", "active_flg", "address_1", "zip_code", "lat", "long", "phone", "country_code", "timezone", "currency", "open_dt", "location_website")
        }
        val actualSquareLocationsData = actual.collect()
        withClue("POS Square refined locations data do not match") {
            val expected = List(
                ("L0P0DJ340FXF0", "#8 Desert Ridge", "PHYSICAL", "ACTIVE", "21001 N Tatum Blvd Suite 34-1130", "85050", "33.677158", "-111.977239", "df2e49446821163cbf58168c436fec5fb2b09b4b12496fdc4e38d3e9784c2790", "US", "America/Phoenix", "USD", "2021-05-13T21:50:55Z", "www.cupbop.com")
            ).toDF("location_id", "location_nm", "location_type", "active_flg", "address_1", "zip_code", "lat", "long", "phone", "country_code", "timezone", "currency", "open_dt", "location_website").collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected
        }
    }

    test("test square partner location transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val locations = List(
            ("L0P0DJ340FXF0", "PHYSICAL", "ACTIVE", null, null, null, null, null, null, null, null, null, null, null),
            ("L0P0DJ340FXF0", "PHYSICAL", "ACTIVE", null, null, null, null, null, null, null, null, null, null, null), // duplicate
            ("ACA0DJ910FPO1","MOBILE", "INACTIVE", null, null, null, null, null, null, null, null, null, null, null),
            ("PQW0DJ340ALS0","some_other_loc_type", "ACTIVE", null, null, null, null, null, null, null, null, null, null, null)
        ).toDF("location_id", "location_type", "active_flg", "location_nm", "address_1", "zip_code", "lat", "long", "phone", "country_code", "timezone", "currency", "open_dt", "location_website")

        // when
        val actual = LocationsProcessor.transformLocations(locations, Seq.empty[(String, String, String, String)].toDF("zip_code", "state", "city", "region"), cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("location_id", "location_type", "active_flg", "location_nm", "address_1", "zip_code", "lat", "long", "phone", "country_code", "timezone", "currency", "open_dt", "location_website", "cxi_partner_id", "address_2", "fax", "parent_location_id", "extended_attr", "state", "city", "region")
        }
        val actualSquareLocationsData = actual.collect()
        withClue("POS Square refined categories data do not match") {
            val expected = List(
                ("L0P0DJ340FXF0", 1, 1, null, null, null, null, null, null, null, null, null, null, null, cxiPartnerId, null, null, null, null, null, null, null),
                ("ACA0DJ910FPO1", 6, 0, null, null, null, null, null, null, null, null, null, null, null, cxiPartnerId, null, null, null, null, null, null, null),
                ("PQW0DJ340ALS0", 0, 1, null, null, null, null, null, null, null, null, null, null, null, cxiPartnerId, null, null, null, null, null, null, null)
            ).toDF("location_id", "location_type", "active_flg", "location_nm", "address_1", "zip_code", "lat", "long", "phone", "country_code", "timezone", "currency", "open_dt", "location_website", "cxi_partner_id", "address_2", "city", "state", "region", "fax", "parent_location_id", "extended_attr").collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected
        }
    }

}
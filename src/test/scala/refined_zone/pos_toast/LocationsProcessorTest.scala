package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.pos_toast.LocationsProcessorTest.{LocationReadOutputModel, LocationTransformOutputModel}
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class LocationsProcessorTest extends BaseSparkBatchJobTest {

    test("test toast partner location read") {
        // given
        import spark.implicits._
        val locations = List(
            (
                s"""
                    {
                       "general":{
                          "closeoutHour":1,
                          "description":"398 7th Street NW Washington, DC 20004",
                          "locationCode":"6",
                          "locationName":"Penn Quarter",
                          "managementGroupGuid":"327841ee-0ce2-4a33-9256-341fadd8e269",
                          "name":"Protein Bar & Kitchen",
                          "timeZone":"America/New_York"
                       },
                       "guid":"d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                       "location":{
                          "address1":"398 7th Street NW",
                          "address2":"",
                          "city":"Washington",
                          "country":"US",
                          "latitude":38.8946484,
                          "longitude":-77.022111,
                          "phone":"2026219574",
                          "stateCode":"DC",
                          "zipCode":"20004"
                       },
                       "urls":{
                          "checkGiftCard":"https://www.toasttab.com/fake-short-url/findcard",
                          "facebook":"",
                          "orderOnline":"https://www.toasttab.com/fake-short-url/",
                          "purchaseGiftCard":"https://www.toasttab.com/fake-short-url/giftcards",
                          "twitter":"https://twitter.com/",
                          "website":"http://www.theproteinbar.com/location-details?loc=pennquarter7thd"
                       }
                    }
                """,
                "restaurants",
                "2022-02-24"
            ),
            (
                s"""
                    {
                       "general":{
                          "closeoutHour":1,
                          "description":"398 7th Street NW Washington, DC 20004",
                          "locationCode":"6",
                          "managementGroupGuid":"327841ee-0ce2-4a33-9256-341fadd8e269",
                          "name":"Protein Bar & Kitchen",
                          "timeZone":"America/New_York"
                       },
                       "guid":"d8858e8e-67bc-4bd5-9b48-be29682aa03da",
                       "location":{
                          "address1":"398 7th Street NW",
                          "address2":"",
                          "city":"Washington",
                          "country":"US",
                          "latitude":38.8946484,
                          "longitude":-77.022111,
                          "phone":"2026219574",
                          "stateCode":"DC",
                          "zipCode":"20004"
                       },
                       "urls":{
                          "checkGiftCard":"https://www.toasttab.com/fake-short-url/findcard",
                          "facebook":"",
                          "orderOnline":"https://www.toasttab.com/fake-short-url/",
                          "purchaseGiftCard":"https://www.toasttab.com/fake-short-url/giftcards",
                          "twitter":"https://twitter.com/",
                          "website":"http://www.theproteinbar.com/location-details?loc=pennquarter7thd"
                       }
                    }
                """,
                "restaurants",
                "2022-02-24"
            ),
            (
                s"""
                   {
                       "id":"P910DJ120AQA0",
                       "name":"#9 Desert Ridge",
                       "type":"PHYSICAL",
                       "status":"ACTIVE",
                       "address":{
                          "address_line_1":"33202 N Mamum Blvd Suite 19-1829",
                          "postal_code":"95060",
                          "country":"US"
                       },
                       "coordinates":{
                          "latitude":"44.677158",
                          "longitude":"-222.977239"
                       },
                       "phone_number":"apqe49446821163cbf58168c436fec5fb2b09b4b12496fdc4e38d3e9784c2102",
                       "timezone":"America/Phoenix",
                       "currency":"USD",
                       "created_at":"2021-05-13T21:50:55Z",
                       "website_url":"www.cupbop.com"
                    }
                   """,
                "restaurants",
                "2022-02-25"
            ) // duplicate with diff date that gets filtered out
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "locations"
        locations.createOrReplaceTempView(tableName)

        // when
        val actual = LocationsProcessor.readLocations(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "location_id",
                "address_1",
                "address_2",
                "lat",
                "long",
                "phone",
                "zip_code",
                "location_website",
                "timezone",
                "country_code",
                "location_nm"
            )
        }
        val actualToastLocationsData = actual.collect()
        withClue("POS Toast refined locations data do not match") {
            val expected = spark
                .createDataset(
                    List(
                        LocationReadOutputModel(
                            location_id = "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                            address_1 = "398 7th Street NW",
                            address_2 = "",
                            lat = Some(38.8946484),
                            `lon` = Some(-77.022111),
                            phone = "2026219574",
                            zip_code = "20004",
                            location_website = "http://www.theproteinbar.com/location-details?loc=pennquarter7thd",
                            timezone = "America/New_York",
                            country_code = "US",
                            location_nm = "Penn Quarter,Protein Bar & Kitchen"
                        ),
                        LocationReadOutputModel(
                            location_id = "d8858e8e-67bc-4bd5-9b48-be29682aa03da",
                            address_1 = "398 7th Street NW",
                            address_2 = "",
                            lat = Some(38.8946484),
                            `lon` = Some(-77.022111),
                            phone = "2026219574",
                            zip_code = "20004",
                            location_website = "http://www.theproteinbar.com/location-details?loc=pennquarter7thd",
                            timezone = "America/New_York",
                            country_code = "US",
                            location_nm = "Protein Bar & Kitchen"
                        )
                    )
                )
                .withColumnRenamed("lon", "long")
                .collect()
            actualToastLocationsData.length should equal(expected.length)
            actualToastLocationsData should contain theSameElementsAs expected
        }
    }

    test("test toast partner location transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val loc1 = LocationReadOutputModel(
            location_id = "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
            address_1 = "398 7th Street NW",
            address_2 = "",
            lat = Some(38.8946484),
            `lon` = Some(-77.022111),
            phone = "2026219574",
            zip_code = "20004",
            location_website = "http://www.theproteinbar.com/location-details?loc=pennquarter7thd",
            timezone = "America/New_York",
            country_code = "US",
            location_nm = "Penn Quarter,Protein Bar & Kitchen"
        )
        val locations = List(
            loc1,
            loc1.copy() // duplicate
        )

        val postalCodes = Seq(
            ("20004", "US", "NY", "Central")
        ).toDF("zip_code", "state", "city", "region")

        // when
        val actual =
            LocationsProcessor.transformLocations(
                spark.createDataset(locations).withColumnRenamed("lon", "long").toDF(),
                postalCodes,
                cxiPartnerId
            )

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "zip_code",
                "location_id",
                "address_1",
                "address_2",
                "lat",
                "long",
                "phone",
                "location_website",
                "timezone",
                "country_code",
                "location_nm",
                "fax",
                "parent_location_id",
                "extended_attr",
                "currency",
                "cxi_partner_id",
                "active_flg",
                "location_type",
                "open_dt",
                "state",
                "city",
                "region"
            )
        }
        val actualToastLocationsData = actual.collect()
        withClue("POS Toast refined locations data do not match") {
            val expected = spark
                .createDataset(
                    List(
                        LocationTransformOutputModel(
                            location_id = "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                            address_1 = "398 7th Street NW",
                            address_2 = "",
                            lat = Some(38.8946484),
                            `lon` = Some(-77.022111),
                            phone = "2026219574",
                            zip_code = "20004",
                            location_website = "http://www.theproteinbar.com/location-details?loc=pennquarter7thd",
                            timezone = "America/New_York",
                            country_code = "US",
                            location_nm = "Penn Quarter,Protein Bar & Kitchen",
                            cxi_partner_id = cxiPartnerId,
                            currency = "USD",
                            active_flg = "1",
                            location_type = "1",
                            city = "NY",
                            state = "US",
                            region = "Central"
                        )
                    )
                )
                .withColumnRenamed("lon", "long")
                .select(actual.columns.map(col): _*) // to have the same column order
                .collect()
            actualToastLocationsData.length should equal(expected.length)
            actualToastLocationsData should contain theSameElementsAs expected
        }
    }

}

object LocationsProcessorTest {
    case class LocationReadOutputModel(
        location_id: String = null,
        address_1: String = null,
        address_2: String = null,
        lat: Option[Double] = None,
        `lon`: Option[Double] = None,
        phone: String = null,
        zip_code: String = null,
        location_website: String = null,
        timezone: String = null,
        country_code: String = null,
        location_nm: String = null
    )

    case class LocationTransformOutputModel(
        location_id: String = null,
        address_1: String = null,
        address_2: String = null,
        lat: Option[Double] = None,
        `lon`: Option[Double] = None,
        phone: String = null,
        zip_code: String = null,
        location_website: String = null,
        timezone: String = null,
        country_code: String = null,
        location_nm: String = null,
        fax: String = null,
        parent_location_id: String = null,
        extended_attr: String = null,
        currency: String = null,
        cxi_partner_id: String,
        active_flg: String = null,
        location_type: String = null,
        open_dt: String = null,
        city: String = null,
        state: String = null,
        region: String = null
    )
}

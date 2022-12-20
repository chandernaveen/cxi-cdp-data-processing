package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.LocationsProcessor.{ActiveFlagDefault, CurrencyDefault, LocationTypeDefault}
import refined_zone.pos_parbrink.processor.LocationsProcessorTest.{
    LocationOnRead,
    LocationRaw,
    LocationRefined,
    PostalCode
}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class LocationsProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val location_1_onRead = LocationOnRead(
        location_id = "loc_id_1",
        location_nm = "Miguel's Jr. - Original",
        address_1 = "1039 West 6th St.",
        address_2 = null,
        city = "Corona\t",
        state_code = "CA",
        zip_code = "92882",
        lat = "33.877219",
        longitude = "-117.581545",
        phone = "(951) 270-3551",
        fax = null,
        country_code = "US",
        timezone = "3"
    )

    val location_2_onRead = LocationOnRead(
        location_id = "loc_id_2",
        location_nm = "* Darrin Heisey",
        address_1 = "11545 W. Bernardo Ct",
        address_2 = "Suite 150",
        city = null,
        state_code = null,
        zip_code = "92881",
        lat = "33.025114",
        longitude = "-117.082445",
        phone = "Home Phone",
        fax = "1234567890",
        country_code = "US",
        timezone = "999"
    )
    val location_3_onRead = LocationOnRead(
        location_id = "loc_id_3",
        location_nm = "* Darrin Heisey",
        address_1 = "11545 W. Bernardo Ct",
        address_2 = "Suite 150",
        city = "Test \nCity\t One",
        state_code = null,
        zip_code = "92881",
        lat = "33.025114",
        longitude = "-117.082445",
        phone = "Home Phone",
        fax = "1234567890",
        country_code = "US",
        timezone = "999"
    )

    test("read Parbrink locations") {

        // given
        val location_1 = LocationRaw(
            Address1 = "1039 West 6th St.",
            Address2 = None,
            City = Some("Corona\t"),
            Country = "US",
            Latitude = 33.877219,
            Longitude = -117.581545,
            Name = "Miguel's Jr. - Original",
            Phone = "(951) 270-3551",
            Fax = None,
            State = Some("CA"),
            TimeZone = 3,
            Zip = "92882" // zip code is absent from the Postal codes table, the region should be null
        )

        val location_2 = LocationRaw(
            Address1 = "11545 W. Bernardo Ct",
            Address2 = Some("Suite 150"),
            City = None, // not specified, should be taken from postal_codes table
            Country = "US",
            Latitude = 33.025114,
            Longitude = -117.082445,
            Name = "* Darrin Heisey",
            Phone = "Home Phone", // cannot normalize - should be null
            Fax = Some("1234567890"),
            State = None, // not specified, should be taken from postal_codes table
            TimeZone = 999, // invalid TZ code, should be null
            Zip = "92881"
        )

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(location_1),
                record_type = ParbrinkRecordType.Locations.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/options_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(location_1),
                record_type = ParbrinkRecordType.Locations.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/options_1_copy.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(location_2),
                record_type = ParbrinkRecordType.Locations.value,
                location_id = "loc_id_2",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/options_2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = "orders", // not a location type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(location_1),
                record_type = ParbrinkRecordType.Locations.value,
                location_id = "loc_id_4",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/options_3.json",
                cxi_id = "cxi_id_4"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val locationsOnRead = LocationsProcessor.readLocations(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink locations data does not match") {
            val expected = Seq(
                location_1_onRead,
                location_1_onRead, // emulate duplicated data coming from different files
                location_2_onRead
            ).toDF()
                .withColumnRenamed("longitude", "long")
            assert("Column size not Equal", expected.columns.size, locationsOnRead.columns.size)
            assertDataFrameEquals(expected, locationsOnRead)
        }
    }

    test("transform Parbrink locations") {

        // given
        val locationsOnRead = Seq(
            location_1_onRead,
            location_1_onRead, // emulate duplicated data coming from different files
            location_2_onRead,
            location_3_onRead
        ).toDF()
            .withColumnRenamed("longitude", "long")

        val postalCodes = Seq(
            PostalCode(
                postal_code = "92881",
                state = "California",
                county = "Riverside",
                city = "Corona",
                lat = 33.82399,
                lng = 0.0,
                region = "SouthWest",
                state_code = "CA"
            )
        ).toDF()
        val postalCodesTable = "tempPostalCodesTable"
        postalCodes.createOrReplaceTempView(postalCodesTable)

        // when
        val transformedLocations =
            LocationsProcessor.transformLocations(spark, locationsOnRead, postalCodesTable, cxiPartnerId)

        // then
        withClue("transformed Parbrink locations do not match") {
            val expected = Seq(
                LocationRefined(
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
                    timezone = "America/Los_Angeles",
                    extended_attr = null
                ),
                LocationRefined(
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
                ),
                LocationRefined(
                    location_id = "loc_id_3",
                    cxi_partner_id = cxiPartnerId,
                    location_type = LocationTypeDefault,
                    location_nm = "* Darrin Heisey",
                    location_website = null,
                    active_flg = ActiveFlagDefault,
                    address_1 = "11545 W. Bernardo Ct",
                    address_2 = "Suite 150",
                    city = "Test City One",
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
            ).toDF()
                .withColumnRenamed("longitude", "long")
            assert("Column size not Equal", expected.columns.size, transformedLocations.columns.size)
            assertDataFrameDataEquals(expected, transformedLocations)
        }
    }

}

object LocationsProcessorTest {

    case class LocationRaw(
        Address1: String,
        Address2: Option[String],
        City: Option[String],
        Country: String,
        Latitude: Double,
        Longitude: Double,
        Name: String,
        Phone: String,
        Fax: Option[String],
        State: Option[String],
        TimeZone: Int,
        Zip: String
    )

    case class LocationOnRead(
        location_id: String,
        location_nm: String,
        address_1: String,
        address_2: String,
        city: String,
        state_code: String,
        zip_code: String,
        lat: String,
        longitude: String,
        phone: String,
        fax: String,
        country_code: String,
        timezone: String
    )

    case class PostalCode(
        postal_code: String,
        state: String,
        county: String,
        city: String,
        lat: Double,
        lng: Double,
        region: String,
        state_code: String
    )

    case class LocationRefined(
        location_id: String,
        cxi_partner_id: String,
        location_type: String,
        location_nm: String,
        location_website: String,
        active_flg: String,
        address_1: String,
        address_2: String,
        city: String,
        state_code: String,
        region: String,
        zip_code: String,
        lat: String,
        longitude: String,
        phone: String,
        fax: String,
        country_code: String,
        parent_location_id: String,
        currency: String,
        open_dt: java.sql.Timestamp,
        timezone: String,
        extended_attr: String
    )
}

package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.HouseAccountsProcessorTest.{
    HouseAccountOnRead,
    HouseAccountRaw,
    HouseAccountRefined
}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class HouseAccountsProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val houseAccount_1_onRead =
        HouseAccountOnRead(
            location_id = "loc_id_1",
            house_account_id = 1,
            account_number = "1a",
            address_1 = "750 N. St. Paul Street",
            address_2 = "3rd Floor",
            balance = 123.45,
            city = "Dallas",
            email_address = "hashed_email_1",
            is_enforced_limit = true,
            first_name = "John",
            last_name = "Smith",
            limit = 499.99,
            name = "John Smith",
            phone_number = "hashed_phone_1",
            state = "TX",
            zip = "75201-4321"
        )

    val houseAccount_2_onRead =
        HouseAccountOnRead(
            location_id = "loc_id_1",
            house_account_id = 2,
            account_number = "2b",
            address_1 = "Washington Blvd",
            address_2 = "201 W",
            balance = 345.1,
            city = "Los Angeles",
            email_address = "hashed_email_2",
            is_enforced_limit = false,
            first_name = "Paul",
            last_name = "O'Sullivan",
            limit = 0,
            name = "Paul O'Sullivan",
            phone_number = "hashed_phone_2",
            state = "CA",
            zip = "90007"
        )

    test("read Parbrink house accounts") {

        // given
        val houseAccount_1 = HouseAccountRaw(
            Id = 1,
            Active = true,
            AccountNumber = "1a",
            Address1 = "750 N. St. Paul Street",
            Address2 = "3rd Floor",
            Balance = 123.45,
            City = "Dallas",
            EmailAddress = "hashed_email_1",
            EnforceLimit = true,
            FirstName = "John",
            LastName = "Smith",
            Limit = 499.99,
            Name = "John Smith",
            PhoneNumber = "hashed_phone_1",
            State = "TX",
            Zip = "75201-4321"
        )

        val houseAccount_2 = HouseAccountRaw(
            Id = 2,
            Active = true,
            AccountNumber = "2b",
            Address1 = "Washington Blvd",
            Address2 = "201 W",
            Balance = 345.1,
            City = "Los Angeles",
            EmailAddress = "hashed_email_2",
            EnforceLimit = false,
            FirstName = "Paul",
            LastName = "O'Sullivan",
            Limit = 0,
            Name = "Paul O'Sullivan",
            PhoneNumber = "hashed_phone_2",
            State = "CA",
            Zip = "90007"
        )

        val houseAccount_3 = HouseAccountRaw(
            Id = 3,
            Active = false,
            AccountNumber = "3c",
            Address1 = "Amphitheatre Parkway",
            Address2 = "1600",
            Balance = 0,
            City = "Mountain View",
            EmailAddress = "hashed_email_3",
            EnforceLimit = false,
            FirstName = "Stephen",
            LastName = "White",
            Limit = 0,
            Name = "Stephen White",
            PhoneNumber = "hashed_phone_3",
            State = "CA",
            Zip = "94043"
        )

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(houseAccount_1),
                record_type = ParbrinkRecordType.HouseAccounts.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccounts_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(houseAccount_1),
                record_type = ParbrinkRecordType.HouseAccounts.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccounts_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(houseAccount_2),
                record_type = ParbrinkRecordType.HouseAccounts.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccounts_2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = "orders", // not an HouseAccount type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel( // inactive, should be ignored
                record_value = Serialization.write(houseAccount_3),
                record_type = ParbrinkRecordType.HouseAccounts.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccounts_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(houseAccount_1.copy(Id = 4)),
                record_type = ParbrinkRecordType.HouseAccounts.value,
                location_id = "loc_id_2",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/houseAccounts_1.json",
                cxi_id = "cxi_id_1"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val houseAccountsOnRead = HouseAccountsProcessor.readHouseAccounts(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink house accounts data does not match") {
            val expected = Seq(
                houseAccount_1_onRead,
                houseAccount_1_onRead, // emulate duplicated data coming from different files
                houseAccount_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, houseAccountsOnRead.columns.size)
            assertDataFrameDataEquals(expected, houseAccountsOnRead)
        }
    }

    test("transform Parbrink house accounts") {

        // given
        val houseAccountsOnRead = Seq(
            houseAccount_1_onRead,
            houseAccount_1_onRead, // emulate duplicated data coming from different files
            houseAccount_2_onRead
        ).toDF()

        // when
        val transformedHouseAccounts = HouseAccountsProcessor.transformHouseAccounts(houseAccountsOnRead, cxiPartnerId)

        // then
        withClue("transformed Parbrink house accounts do not match") {
            val expected = Seq(
                HouseAccountRefined(
                    location_id = "loc_id_1",
                    cxi_partner_id = cxiPartnerId,
                    house_account_id = 1,
                    account_number = "1a",
                    address_1 = "750 N. St. Paul Street",
                    address_2 = "3rd Floor",
                    balance = 123.45,
                    city = "Dallas",
                    email_address = "hashed_email_1",
                    is_enforced_limit = true,
                    first_name = "John",
                    last_name = "Smith",
                    limit = 499.99,
                    name = "John Smith",
                    phone_number = "hashed_phone_1",
                    state = "TX",
                    zip = "75201"
                ),
                HouseAccountRefined(
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    house_account_id = 2,
                    account_number = "2b",
                    address_1 = "Washington Blvd",
                    address_2 = "201 W",
                    balance = 345.1,
                    city = "Los Angeles",
                    email_address = "hashed_email_2",
                    is_enforced_limit = false,
                    first_name = "Paul",
                    last_name = "O'Sullivan",
                    limit = 0,
                    name = "Paul O'Sullivan",
                    phone_number = "hashed_phone_2",
                    state = "CA",
                    zip = "90007"
                )
            ).toDF()
            assert("Column size not Equal", expected.columns.size, transformedHouseAccounts.columns.size)
            assertDataFrameDataEquals(expected, transformedHouseAccounts)
        }
    }

}

object HouseAccountsProcessorTest {

    case class HouseAccountRaw(
        Id: Int,
        Active: Boolean,
        AccountNumber: String,
        Address1: String,
        Address2: String,
        Address3: String = null,
        Address4: String = null,
        Balance: Double,
        City: String,
        EmailAddress: String,
        EnforceLimit: Boolean,
        FirstName: String,
        LastName: String,
        Limit: Double,
        MiddleName: String = null,
        Name: String,
        PhoneNumber: String,
        State: String,
        Zip: String
    )
    case class HouseAccountOnRead(
        location_id: String,
        house_account_id: Int,
        account_number: String,
        address_1: String,
        address_2: String,
        address_3: String = null,
        address_4: String = null,
        balance: Double,
        city: String,
        email_address: String,
        is_enforced_limit: Boolean,
        first_name: String,
        last_name: String,
        limit: Double,
        middle_name: String = null,
        name: String,
        phone_number: String,
        state: String,
        zip: String
    )

    case class HouseAccountRefined(
        location_id: String,
        cxi_partner_id: String,
        house_account_id: Int,
        account_number: String,
        address_1: String,
        address_2: String,
        address_3: String = null,
        address_4: String = null,
        balance: Double,
        city: String,
        email_address: String,
        is_enforced_limit: Boolean,
        first_name: String,
        last_name: String,
        limit: Double,
        middle_name: String = null,
        name: String,
        phone_number: String,
        state: String,
        zip: String
    )
}

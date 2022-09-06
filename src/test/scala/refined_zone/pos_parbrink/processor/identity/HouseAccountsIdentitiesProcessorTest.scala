package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Type, Weight}
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.identity.HouseAccountsIdentitiesProcessorTest.{
    HouseAccountChargesOnRead,
    HouseAccountChargesRaw,
    HouseAccountOnRead,
    HouseAccountRaw
}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class HouseAccountsIdentitiesProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val houseAccountCharges_1_onRead =
        HouseAccountChargesOnRead(location_id = "loc_id_1", house_account_id = 3, order_id = 93)
    val houseAccountCharges_2_onRead =
        HouseAccountChargesOnRead(location_id = "loc_id_1", house_account_id = 2, order_id = 92)

    val houseAccount_1_onRead = HouseAccountOnRead(
        location_id = "loc_id_1",
        house_account_id = 1,
        email_address = "hashed_email_1",
        phone_number = "hashed_phone_1"
    )

    val houseAccount_2_onRead = HouseAccountOnRead(
        location_id = "loc_id_1",
        house_account_id = 2,
        email_address = "hashed_email_2",
        phone_number = "hashed_phone_2"
    )

    test("read Parbrink House accounts charges") {
        // given
        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(HouseAccountChargesRaw(AccountId = 3, OrderId = 93)),
                record_type = ParbrinkRecordType.HouseAccountCharges.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccountsCharges_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(HouseAccountChargesRaw(AccountId = 3, OrderId = 93)),
                record_type = ParbrinkRecordType.HouseAccountCharges.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccountsCharges_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = ParbrinkRecordType.Orders.value, // not a House Accounts Charges type
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(HouseAccountChargesRaw(AccountId = 2, OrderId = 92)),
                record_type = ParbrinkRecordType.HouseAccountCharges.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/houseAccountsCharges_2.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // another feed date
                record_value = Serialization.write(HouseAccountChargesRaw(AccountId = 3, OrderId = 93)),
                record_type = ParbrinkRecordType.HouseAccountCharges.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05",
                file_name = "location=loc_id_1/houseAccountsCharges_1.json",
                cxi_id = "cxi_id_1"
            )
        ).toDF()

        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val actual = HouseAccountsIdentitiesProcessor.readHouseAccountCharges(spark, "2022-02-24", rawTable)

        // then
        withClue("read Parbrink house accounts charges data does not match") {
            val expected = Seq(
                houseAccountCharges_1_onRead,
                houseAccountCharges_1_onRead, // emulate duplicated data coming from different files
                houseAccountCharges_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }
    }

    test("read Parbrink house accounts") {

        // given
        val houseAccount_1 = HouseAccountRaw(Id = 1, EmailAddress = "hashed_email_1", PhoneNumber = "hashed_phone_1")
        val houseAccount_2 = HouseAccountRaw(Id = 2, EmailAddress = "hashed_email_2", PhoneNumber = "hashed_phone_2")
        val houseAccount_3 =
            HouseAccountRaw(Id = 3, EmailAddress = "hashed_email_3", PhoneNumber = "hashed_phone_3", Active = false)

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
        val houseAccountsOnRead = HouseAccountsIdentitiesProcessor.readHouseAccounts(spark, "2022-02-24", rawTable)

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

    test("transform and compute identities from Parbrink house accounts") {
        // given
        val houseAccounts = Seq(
            houseAccount_1_onRead,
            houseAccount_1_onRead, // emulate duplicated data
            houseAccount_2_onRead
        ).toDF()
        val houseAccountCharges = Seq(
            houseAccountCharges_1_onRead,
            houseAccountCharges_2_onRead,
            houseAccountCharges_2_onRead // emulate duplicated data
        ).toDF()

        // when
        val houseAccountsWithOrder =
            HouseAccountsIdentitiesProcessor.transformHouseAccounts(houseAccounts, houseAccountCharges)

        // then
        withClue("transformed Parbrink house accounts data does not match") {
            val expected = Seq(
                ("loc_id_1", "hashed_email_1", "hashed_phone_1", null),
                ("loc_id_1", "hashed_email_2", "hashed_phone_2", "92")
            ).toDF("location_id", "email_address", "phone_number", "order_id")
            assert("Column size not Equal", expected.columns.size, houseAccountsWithOrder.columns.size)
            assertDataFrameDataEquals(expected, houseAccountsWithOrder)
        }

        // when
        val identities = HouseAccountsIdentitiesProcessor.computeIdentitiesFromHouseAccounts(houseAccountsWithOrder)

        // then
        withClue("Parbrink house accounts identities data does not match") {
            val expected = Seq(
                (null, "loc_id_1", "hashed_email_1", IdentityType.Email.code, 3),
                ("92", "loc_id_1", "hashed_email_2", IdentityType.Email.code, 3),
                (null, "loc_id_1", "hashed_phone_1", IdentityType.Phone.code, 3),
                ("92", "loc_id_1", "hashed_phone_2", IdentityType.Phone.code, 3)
            ).toDF("order_id", "location_id", CxiIdentityId, Type, Weight)
            assert("Column size not Equal", expected.columns.size, identities.columns.size)
            assertDataFrameDataEquals(expected, identities)
        }

    }

}

object HouseAccountsIdentitiesProcessorTest {

    case class HouseAccountChargesRaw(AccountId: Int, OrderId: Long)
    case class HouseAccountChargesOnRead(location_id: String, house_account_id: Int, order_id: Long)
    case class HouseAccountRaw(Id: Int, EmailAddress: String, PhoneNumber: String, Active: Boolean = true)
    case class HouseAccountOnRead(
        location_id: String,
        house_account_id: Int,
        email_address: String,
        phone_number: String
    )

}

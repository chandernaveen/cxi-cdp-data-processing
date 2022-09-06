package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import com.cxi.cdp.data_processing.refined_zone.pos_parbrink.model.ParbrinkRawModels.PhoneNumber
import refined_zone.hub.identity.model.IdentityType
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.CustomersProcessorTest.{
    CustomerOnRead,
    CustomerRaw,
    CustomerRefined,
    PrivacyLookup
}
import support.crypto_shredding.hashing.Hash
import support.utils.crypto_shredding.CryptoShreddingTransformOnly
import support.utils.DateTimeTestUtils.{sqlDate, sqlTimestamp}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class CustomersProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"
    val feedDate = "2022-02-24"

    val phoneNumbers_1 = Seq(PhoneNumber(AreaCode = "813", Extension = null, Id = 1, Number = "hashed_phone_1"))
    val phoneNumbers_2 = Seq(
        PhoneNumber(AreaCode = "949", Extension = null, Id = 1, Number = "hashed_phone_2"),
        PhoneNumber(AreaCode = "951", Extension = null, Id = 2, Number = "hashed_phone_3"),
        PhoneNumber(
            AreaCode = "714",
            Extension = null,
            Id = 3,
            Number = "hashed_phone_4"
        ) // not present in privacy table
    )

    val Customer_1_onRead = CustomerOnRead(
        customer_id = "customer_1",
        email_address = "hashed_email_1",
        phones = Serialization.write(phoneNumbers_1),
        first_name = "Alane",
        last_name = "First",
        created_at = "2020-05-12T17:27:14.03"
    )

    val Customer_2_onRead = CustomerOnRead(
        customer_id = "customer_2",
        email_address = "hashed_email_2",
        phones = Serialization.write(phoneNumbers_2),
        first_name = "Darryl",
        last_name = "Second",
        created_at = "2020-05-01T22:58:09.183"
    )

    val Customer_3_onRead = CustomerOnRead(
        customer_id = "customer_3",
        email_address = "hashed_email_3",
        phones = "[]",
        first_name = "Diane",
        last_name = "Third",
        created_at = "2020-05-01T23:27:57.3"
    )

    test("read Parbrink Customers") {

        // given
        val customer_1 = CustomerRaw(
            IsDisabled = false,
            Id = "customer_1",
            EmailAddress = "hashed_email_1",
            PhoneNumbers = phoneNumbers_1,
            FirstName = "Alane",
            LastName = "First",
            RegistrationTime = "2020-05-12T17:27:14.03"
        )

        val customer_2 = CustomerRaw(
            IsDisabled = false,
            Id = "customer_2",
            EmailAddress = "hashed_email_2",
            phoneNumbers_2,
            FirstName = "Darryl",
            LastName = "Second",
            RegistrationTime = "2020-05-01T22:58:09.183"
        )

        val customer_3 = CustomerRaw(
            IsDisabled = false,
            Id = "customer_3",
            EmailAddress = "hashed_email_3",
            PhoneNumbers = Seq(),
            FirstName = "Diane",
            LastName = "Third",
            RegistrationTime = "2020-05-01T23:27:57.3"
        )

        val customer_4 = CustomerRaw( // IsDisabled = true, should be ignored
            IsDisabled = true,
            Id = "customer_4",
            EmailAddress = "hashed_email_4",
            PhoneNumbers = Seq(),
            FirstName = "Kevin",
            LastName = "Fourth",
            RegistrationTime = "2020-05-01T23:39:08.997"
        )

        val customer_5 = CustomerRaw(
            IsDisabled = false,
            Id = "customer_5",
            EmailAddress = "hashed_email_5",
            PhoneNumbers = Seq(),
            FirstName = "Crystal",
            LastName = "Fifth",
            RegistrationTime = "2020-05-01T23:27:57.3"
        )

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(customer_1),
                record_type = ParbrinkRecordType.Customers.value,
                location_id = "loc_id_1",
                feed_date = feedDate,
                file_name = "location=loc_id_1/customers_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(customer_1),
                record_type = ParbrinkRecordType.Customers.value,
                location_id = "loc_id_1",
                feed_date = feedDate,
                file_name = "location=loc_id_1/customers_2.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(customer_2),
                record_type = ParbrinkRecordType.Customers.value,
                location_id = "loc_id_1",
                feed_date = feedDate,
                file_name = "location=loc_id_1/customers_1.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(customer_3),
                record_type = ParbrinkRecordType.Customers.value,
                location_id = "loc_id_1",
                feed_date = feedDate,
                file_name = "location=loc_id_1/customers_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel( // disabled Customer, should be ignored
                record_value = Serialization.write(customer_4),
                record_type = ParbrinkRecordType.Customers.value,
                location_id = "loc_id_1",
                feed_date = feedDate,
                file_name = "location=loc_id_1/customers_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = "orders", // not a Customer type, should be ignored
                location_id = "loc_id_1",
                feed_date = feedDate,
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(customer_5),
                record_type = ParbrinkRecordType.Customers.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/customers_1.json",
                cxi_id = "cxi_id_4"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val customersOnRead = CustomersProcessor.readCustomers(spark, feedDate, rawTable)

        // then
        withClue("read Parbrink customers data does not match") {
            val expected = Seq(
                Customer_1_onRead,
                Customer_1_onRead, // emulate duplicated data coming from different files
                Customer_2_onRead,
                Customer_3_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, customersOnRead.columns.size)
            assertDataFrameEquals(expected, customersOnRead)
        }
    }

    test("transform Parbrink customers") {

        // given
        val customersOnRead = Seq(
            Customer_1_onRead,
            Customer_1_onRead, // emulate duplicated data coming from different files
            Customer_2_onRead,
            Customer_3_onRead
        ).toDF()

        val privacyLookupPhones = Seq(
            PrivacyLookup(
                cxi_source = "cxi-test",
                identity_type = IdentityType.Phone.value,
                original_value = "2345678",
                hashed_value = "hashed_phone_1",
                feed_date = sqlDate(feedDate),
                run_id = "run_id_1"
            ),
            PrivacyLookup(
                cxi_source = "cxi-test",
                identity_type = IdentityType.Phone.value,
                original_value = "3456789",
                hashed_value = "hashed_phone_2",
                feed_date = sqlDate(feedDate),
                run_id = "run_id_1"
            ),
            PrivacyLookup(
                cxi_source = "cxi-test",
                identity_type = IdentityType.Phone.value,
                original_value = "4567890",
                hashed_value = "hashed_phone_3",
                feed_date = sqlDate(feedDate),
                run_id = "run_id_1"
            ),
            PrivacyLookup(
                cxi_source = "cxi-test",
                identity_type = IdentityType.Phone.value,
                original_value = "2345678",
                hashed_value = "hashed_phone_999", // does not belong to any customer
                feed_date = sqlDate(feedDate),
                run_id = "run_id_1"
            )
        ).toDF()

        // when
        val cryptoShredding = new CryptoShreddingTransformOnly()
        val transformedCustomers =
            CustomersProcessor.transformCustomers(customersOnRead, privacyLookupPhones, cryptoShredding, cxiPartnerId)

        // then
        withClue("transformed Parbrink customers do not match") {
            val expected = Seq(
                CustomerRefined(
                    customer_id = "customer_1",
                    email_address = "hashed_email_1",
                    first_name = "Alane",
                    last_name = "First",
                    created_at = sqlTimestamp("2020-05-12T17:27:14.03Z"),
                    phone_numbers = Seq(Hash.sha256Hash("18132345678", cryptoShredding.SaltTest)),
                    cxi_partner_id = cxiPartnerId,
                    version = null
                ),
                CustomerRefined(
                    customer_id = "customer_2",
                    email_address = "hashed_email_2",
                    first_name = "Darryl",
                    last_name = "Second",
                    created_at = sqlTimestamp("2020-05-01T22:58:09.183Z"),
                    phone_numbers = Seq(
                        Hash.sha256Hash("19493456789", cryptoShredding.SaltTest),
                        Hash.sha256Hash("19514567890", cryptoShredding.SaltTest)
                    ),
                    cxi_partner_id = cxiPartnerId,
                    version = null
                ),
                CustomerRefined(
                    customer_id = "customer_3",
                    email_address = "hashed_email_3",
                    first_name = "Diane",
                    last_name = "Third",
                    created_at = sqlTimestamp("2020-05-01T23:27:57.3Z"),
                    phone_numbers = Seq(),
                    cxi_partner_id = cxiPartnerId,
                    version = null
                )
            ).toDF()
            assert("Column size not Equal", expected.columns.size, transformedCustomers.columns.size)
            assertDataFrameDataEquals(expected, transformedCustomers)
        }
    }

}

object CustomersProcessorTest {

    case class CustomerRaw(
        IsDisabled: Boolean,
        Id: String,
        EmailAddress: String,
        PhoneNumbers: Seq[PhoneNumber],
        FirstName: String,
        LastName: String,
        RegistrationTime: String
    )

    case class CustomerOnRead(
        customer_id: String,
        email_address: String,
        phones: String,
        first_name: String,
        last_name: String,
        created_at: String
    )

    case class PrivacyLookup(
        cxi_source: String,
        identity_type: String,
        original_value: String,
        hashed_value: String,
        feed_date: java.sql.Date,
        run_id: String
    )

    case class CustomerRefined(
        customer_id: String,
        email_address: String,
        first_name: String,
        last_name: String,
        created_at: java.sql.Timestamp,
        phone_numbers: Seq[String],
        cxi_partner_id: String,
        version: String
    )
}

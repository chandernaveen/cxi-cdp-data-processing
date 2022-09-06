package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Type, Weight}
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.identity.CustomersIdentitiesProcessor.ParbrinkCustomerIdentityDelimeter
import refined_zone.pos_parbrink.processor.identity.CustomersIdentitiesProcessorTest.{OrderOnRead, OrderRaw}
import refined_zone.pos_parbrink.processor.CustomersProcessorTest.CustomerRefined
import support.utils.DateTimeTestUtils.sqlTimestamp
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class CustomersIdentitiesProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val order_1_onRead = OrderOnRead("loc_id_1", 111, "customer-1")
    val order_2_onRead = OrderOnRead("loc_id_1", 222, "customer-2")

    test("read orders for computing identities from Parbrink customers") {

        // given
        val order_1 = OrderRaw(111, "customer-1")
        val order_2 = OrderRaw(222, "customer-2")

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(order_1),
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(order_1),
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_2.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(order_2),
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_1"
            )
        ).toDF()

        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val orders = CustomersIdentitiesProcessor.readOrders(spark, "2022-02-24", rawTable)

        // then
        withClue("read Parbrink orders data does not match") {
            val expected = Seq(
                order_1_onRead,
                order_1_onRead, // emulate duplicated data coming from different files
                order_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, orders.columns.size)
            assertDataFrameDataEquals(expected, orders)
        }
    }

    test("compute identities from Parbrink customers") {
        // given
        val orders = Seq(
            order_1_onRead,
            order_1_onRead, // emulate duplicated data coming from different files
            order_2_onRead
        ).toDF()

        val customers = Seq(
            CustomerRefined(
                customer_id = "customer-1",
                email_address = "hashed_email_1",
                first_name = "Alane",
                last_name = "First",
                created_at = sqlTimestamp("2020-05-12T17:27:14.03Z"),
                phone_numbers = Seq("hashed_phone_1", "hashed_phone_2"),
                cxi_partner_id = cxiPartnerId,
                version = null
            ),
            CustomerRefined(
                customer_id = "customer-2",
                email_address = "hashed_email_2",
                first_name = "Darryl",
                last_name = "Second",
                created_at = sqlTimestamp("2020-05-01T22:58:09.183Z"),
                phone_numbers = Seq(),
                cxi_partner_id = cxiPartnerId,
                version = null
            ),
            CustomerRefined(
                customer_id = "customer-3",
                email_address = null,
                first_name = "Diane",
                last_name = "Third",
                created_at = sqlTimestamp("2020-05-01T23:27:57.3Z"),
                phone_numbers = Seq("hashed_phone_3"),
                cxi_partner_id = cxiPartnerId,
                version = null
            ),
            CustomerRefined(
                customer_id = "customer-4",
                email_address = null,
                first_name = "Kevin",
                last_name = "Fourth",
                created_at = sqlTimestamp("2020-05-01T23:27:57.3Z"),
                phone_numbers = Seq(),
                cxi_partner_id = cxiPartnerId,
                version = null
            )
        ).toDF()

        // when
        val actual = CustomersIdentitiesProcessor.computeIdentitiesFromCustomers(customers, orders, cxiPartnerId)

        // then
        withClue("identities from Parbrink customers do not match") {
            val expected = Seq(
                ("111", "loc_id_1", "hashed_email_1", IdentityType.Email.code, 3),
                ("111", "loc_id_1", "hashed_phone_1", IdentityType.Phone.code, 3),
                ("111", "loc_id_1", "hashed_phone_2", IdentityType.Phone.code, 3),
                ("222", "loc_id_1", "hashed_email_2", IdentityType.Email.code, 3),
                (null, null, "hashed_phone_3", IdentityType.Phone.code, 3),
                (
                    "111",
                    "loc_id_1",
                    s"$cxiPartnerId${ParbrinkCustomerIdentityDelimeter}customer-1",
                    IdentityType.ParbrinkCustomerId.code,
                    3
                ),
                (
                    "222",
                    "loc_id_1",
                    s"$cxiPartnerId${ParbrinkCustomerIdentityDelimeter}customer-2",
                    IdentityType.ParbrinkCustomerId.code,
                    3
                ),
                (
                    null,
                    null,
                    s"$cxiPartnerId${ParbrinkCustomerIdentityDelimeter}customer-3",
                    IdentityType.ParbrinkCustomerId.code,
                    3
                ),
                (
                    null,
                    null,
                    s"$cxiPartnerId${ParbrinkCustomerIdentityDelimeter}customer-4",
                    IdentityType.ParbrinkCustomerId.code,
                    3
                )
            ).toDF("order_id", "location_id", CxiIdentityId, Type, Weight)
            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }
    }

}

object CustomersIdentitiesProcessorTest {

    case class OrderRaw(Id: Long, CustomerId: String)
    case class OrderOnRead(location_id: String, order_id: Long, customer_id: String)
}

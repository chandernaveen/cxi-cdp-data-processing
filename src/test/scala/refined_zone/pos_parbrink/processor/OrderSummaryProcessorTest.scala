package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.identity.model.IdentityId
import refined_zone.hub.identity.model.IdentityType.{CardHolderNameTypeNumber, Email, ParbrinkCustomerId, Phone}
import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import refined_zone.pos_parbrink.model.ParbrinkRawModels.{OrderEntry, OrderEntryTax, Payment}
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.identity.PosIdentityProcessorTest.IdentitiesByOrder
import refined_zone.pos_parbrink.processor.OrderSummaryProcessorTest._
import support.utils.DateTimeTestUtils.{sqlDate, sqlTimestamp}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val destination_1_onRead = DestinationOnRead("loc_id_1", "206781", "Drive Thru")
    val destination_2_onRead = DestinationOnRead("loc_id_2", "211200", "Online-PickUp")

    val orderEntry_1 = OrderEntry(
        ItemId = 211481,
        Id = 1,
        GrossSales = 3.99,
        Taxes = Seq(OrderEntryTax(Id = 1, Amount = 0.35, TaxId = 1))
    )

    val orderEntry_2 = OrderEntry(
        ItemId = 214383,
        Id = 2,
        GrossSales = 0,
        Taxes = Seq(OrderEntryTax(Id = 1, Amount = 0, TaxId = 1))
    )

    val orderEntry_3 = OrderEntry(
        ItemId = 211288,
        Id = 1,
        GrossSales = 1.01,
        Taxes = Seq(OrderEntryTax(Id = 1, Amount = 0.01, TaxId = 1))
    )

    val payment_1 = Payment(
        Id = 10,
        IsDeleted = false,
        TenderId = 9,
        CardNumber = "8464",
        CardHolderName = "SILVA/RAUL A              ",
        TipAmount = 0.5
    )

    val payment_2 = Payment(
        Id = 11,
        IsDeleted = false,
        TenderId = 5,
        CardNumber = null,
        CardHolderName = null,
        TipAmount = 0
    )

    val order_1_onRead = OrderOnRead(
        location_id = "loc_id_1",
        ord_id = "3132415954391041",
        ord_desc = "DT 1",
        ord_date = "2022-08-03T00:00:00",
        ord_timestamp = "2022-08-03T10:05:53.960489-07:00",
        is_closed = true,
        destination_id = "206781",
        entries = Serialization.write(Seq(orderEntry_1, orderEntry_2)),
        emp_id = "240005",
        total_taxes_amount = 0.7,
        payments = Serialization.write(Seq(payment_1)),
        ord_sub_total = 7.98,
        ord_total = 8.68
    )

    val order_2_onRead = OrderOnRead(
        location_id = "loc_id_2",
        ord_id = "3132415954391043",
        ord_desc = "DT 3",
        ord_date = "2022-02-24T00:00:00",
        ord_timestamp = "2022-02-24T10:19:39.8269167-07:00",
        is_closed = true,
        destination_id = "999",
        entries = Serialization.write(Seq(orderEntry_3)),
        emp_id = "240005",
        total_taxes_amount = 1.06,
        payments = Serialization.write(Seq(payment_2)),
        ord_sub_total = 12.1,
        ord_total = 13.16
    )

    test("read Parbrink destinations") {

        // given
        val destination_1 = DestinationRaw(206781, "Drive Thru")
        val destination_2 = DestinationRaw(211200, "Online-PickUp")

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(destination_1),
                record_type = ParbrinkRecordType.Destinations.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/destinations_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(destination_1),
                record_type = ParbrinkRecordType.Destinations.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/destinations_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(destination_2),
                record_type = ParbrinkRecordType.Destinations.value,
                location_id = "loc_id_2",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/destinations_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = "orders", // not a destination type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(destination_2),
                record_type = ParbrinkRecordType.Destinations.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/destinations_1.json",
                cxi_id = "cxi_id_4"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val destinationsOnRead = OrderSummaryProcessor.readDestinations(spark, "2022-02-24", rawTable)

        // then
        withClue("read Parbrink destinations data does not match") {
            val expected = Seq(
                destination_1_onRead,
                destination_1_onRead, // emulate duplicated data coming from different files
                destination_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, destinationsOnRead.columns.size)
            assertDataFrameEquals(expected, destinationsOnRead)
        }
    }

    test("read Parbrink orders") {

        // given
        val order_1 = OrderRaw(
            Id = 3132415954391041L,
            Name = "DT 1",
            BusinessDate = "2022-08-03T00:00:00",
            ModifiedTime = "2022-08-03T10:05:53.960489-07:00",
            IsClosed = true,
            DestinationId = 206781,
            Entries = Seq(orderEntry_1, orderEntry_2),
            EmployeeId = 240005,
            Tax = 0.7,
            Payments = Seq(payment_1),
            Subtotal = 7.98,
            Total = 8.68
        )

        val order_2 = OrderRaw(
            Id = 3132415954391043L,
            Name = "DT 3",
            BusinessDate = "2022-02-24T00:00:00",
            ModifiedTime = "2022-02-24T10:19:39.8269167-07:00",
            IsClosed = true,
            DestinationId = 999,
            Entries = Seq(orderEntry_3),
            EmployeeId = 240005,
            Tax = 1.06,
            Payments = Seq(payment_2),
            Subtotal = 12.1,
            Total = 13.16
        )

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
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(order_2),
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_2",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = ParbrinkRecordType.Categories.value, // not an order type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/categories_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(order_2),
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_4"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val ordersOnRead = OrderSummaryProcessor.readOrders(spark, "2022-02-24", rawTable)

        // then
        withClue("read Parbrink orders data does not match") {
            val expected = Seq(
                order_1_onRead,
                order_1_onRead, // emulate duplicated data coming from different files
                order_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, ordersOnRead.columns.size)
            assertDataFrameDataEquals(expected, ordersOnRead)
        }
    }

    test("transform Parbrink orders") {

        // given
        val orders = Seq(
            order_1_onRead,
            order_1_onRead, // emulate duplicated data coming from different files
            order_2_onRead
        ).toDF()

        val destinations = Seq(
            destination_1_onRead,
            destination_1_onRead, // emulate duplicated data coming from different files
            destination_2_onRead
        ).toDF()

        val identitiesByOrder = Seq(
            IdentitiesByOrder(
                "3132415954391041",
                "loc_id_1",
                Seq(
                    IdentityId(Email.code, "hashed_email_1"),
                    IdentityId(Phone.code, "hashed_phone_1"),
                    IdentityId(Phone.code, "hashed_phone_2"),
                    IdentityId(CardHolderNameTypeNumber.code, "hashed_payment_details")
                )
            ),
            IdentitiesByOrder( // should be ignored as there is no order_id/location_id match with order data
                "3132415954391043",
                "loc_id_1",
                Seq(
                    IdentityId(Email.code, "hashed_email_2"),
                    IdentityId(ParbrinkCustomerId.code, "cxi-partner-1_customer-1")
                )
            )
        ).toDF()

        // when
        val actual =
            OrderSummaryProcessor.transformOrders(orders, destinations, identitiesByOrder, cxiPartnerId, "2022-02-24")

        // then
        withClue("transformed Parbrink orders data does not match") {
            val expected = Seq(
                OrderRefined( // order 1, item 1
                    ord_id = "3132415954391041",
                    ord_desc = "DT 1",
                    ord_total = 8.68,
                    ord_date = sqlDate("2022-08-03"),
                    ord_timestamp = sqlTimestamp("2022-08-03T10:05:53.960489-07:00"),
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    ord_state_id = OrderStateType.Completed.code,
                    ord_type = null,
                    emp_id = "240005",
                    total_taxes_amount = 0.7,
                    total_tip_amount = 0.5,
                    tender_ids = Seq("9"),
                    item_quantity = null,
                    item_total = 3.99,
                    discount_id = null,
                    discount_amount = None,
                    dsp_qty = None,
                    dsp_ttl = None,
                    guest_check_line_item_id = "1",
                    line_id = null,
                    item_price_id = null,
                    reason_code_id = null,
                    service_charge_id = null,
                    service_charge_amount = None,
                    ord_sub_total = 7.98,
                    ord_pay_total = 8.68,
                    item_id = "211481",
                    taxes_id = "[1]",
                    taxes_amount = 0.35,
                    ord_originate_channel_id = OrderChannelType.PhysicalLane.code,
                    ord_target_channel_id = OrderChannelType.PhysicalPickup.code,
                    cxi_identity_ids = Seq(
                        IdentityId(Email.code, "hashed_email_1"),
                        IdentityId(Phone.code, "hashed_phone_1"),
                        IdentityId(Phone.code, "hashed_phone_2"),
                        IdentityId(CardHolderNameTypeNumber.code, "hashed_payment_details")
                    ),
                    feed_date = sqlDate("2022-02-24")
                ),
                OrderRefined( // order 1, item 2
                    ord_id = "3132415954391041",
                    ord_desc = "DT 1",
                    ord_total = 8.68,
                    ord_date = sqlDate("2022-08-03"),
                    ord_timestamp = sqlTimestamp("2022-08-03T10:05:53.960489-07:00"),
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    ord_state_id = OrderStateType.Completed.code,
                    ord_type = null,
                    emp_id = "240005",
                    total_taxes_amount = 0.7,
                    total_tip_amount = 0.5,
                    tender_ids = Seq("9"),
                    item_quantity = null,
                    item_total = 0,
                    discount_id = null,
                    discount_amount = None,
                    dsp_qty = None,
                    dsp_ttl = None,
                    guest_check_line_item_id = "2",
                    line_id = null,
                    item_price_id = null,
                    reason_code_id = null,
                    service_charge_id = null,
                    service_charge_amount = None,
                    ord_sub_total = 7.98,
                    ord_pay_total = 8.68,
                    item_id = "214383",
                    taxes_id = "[1]",
                    taxes_amount = 0,
                    ord_originate_channel_id = OrderChannelType.PhysicalLane.code,
                    ord_target_channel_id = OrderChannelType.PhysicalPickup.code,
                    cxi_identity_ids = Seq(
                        IdentityId(Email.code, "hashed_email_1"),
                        IdentityId(Phone.code, "hashed_phone_1"),
                        IdentityId(Phone.code, "hashed_phone_2"),
                        IdentityId(CardHolderNameTypeNumber.code, "hashed_payment_details")
                    ),
                    feed_date = sqlDate("2022-02-24")
                ),
                OrderRefined( // order 2
                    ord_id = "3132415954391043",
                    ord_desc = "DT 3",
                    ord_total = 13.16,
                    ord_date = sqlDate("2022-02-24"),
                    ord_timestamp = sqlTimestamp("2022-02-24T10:19:39.8269167-07:00"),
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_2",
                    ord_state_id = OrderStateType.Completed.code,
                    ord_type = null,
                    emp_id = "240005",
                    total_taxes_amount = 1.06,
                    total_tip_amount = 0,
                    tender_ids = Seq("5"),
                    item_quantity = null,
                    item_total = 1.01,
                    discount_id = null,
                    discount_amount = None,
                    dsp_qty = None,
                    dsp_ttl = None,
                    guest_check_line_item_id = "1",
                    line_id = null,
                    item_price_id = null,
                    reason_code_id = null,
                    service_charge_id = null,
                    service_charge_amount = None,
                    ord_sub_total = 12.1,
                    ord_pay_total = 13.16,
                    item_id = "211288",
                    taxes_id = "[1]",
                    taxes_amount = 0.01,
                    ord_originate_channel_id = OrderChannelType.Unknown.code,
                    ord_target_channel_id = OrderChannelType.Unknown.code,
                    cxi_identity_ids = null,
                    feed_date = sqlDate("2022-02-24")
                )
            ).toDF()

            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }

    }

}

object OrderSummaryProcessorTest {

    case class DestinationRaw(Id: Int, Name: String)

    case class DestinationOnRead(location_id: String, destination_id: String, destination_name: String)

    case class OrderRaw(
        Id: Long,
        Name: String,
        BusinessDate: String,
        ModifiedTime: String,
        IsClosed: Boolean,
        DestinationId: Int,
        Entries: Seq[OrderEntry],
        EmployeeId: Int,
        Tax: Double,
        Payments: Seq[Payment],
        Subtotal: Double,
        Total: Double
    )

    case class OrderOnRead(
        location_id: String,
        ord_id: String,
        ord_desc: String,
        ord_date: String,
        ord_timestamp: String,
        is_closed: Boolean,
        destination_id: String,
        entries: String,
        emp_id: String,
        total_taxes_amount: Double,
        payments: String,
        ord_sub_total: Double,
        ord_total: Double
    )

    case class OrderRefined(
        ord_id: String,
        ord_desc: String,
        ord_total: Double,
        ord_date: java.sql.Date,
        ord_timestamp: java.sql.Timestamp,
        cxi_partner_id: String,
        location_id: String,
        ord_state_id: Int,
        ord_type: String,
        emp_id: String,
        total_taxes_amount: Double,
        total_tip_amount: Double,
        tender_ids: Seq[String],
        item_quantity: Option[Int],
        item_total: Double,
        discount_id: String,
        discount_amount: Option[Double],
        dsp_qty: Option[Int],
        dsp_ttl: Option[Double],
        guest_check_line_item_id: String,
        line_id: String,
        item_price_id: String,
        reason_code_id: String,
        service_charge_id: String,
        service_charge_amount: Option[Double],
        ord_sub_total: Double,
        ord_pay_total: Double,
        item_id: String,
        taxes_id: String,
        taxes_amount: Double,
        ord_originate_channel_id: Int,
        ord_target_channel_id: Int,
        cxi_identity_ids: Seq[IdentityId],
        feed_date: java.sql.Date
    )

}

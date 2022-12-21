package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.model.{CardBrandType, ParbrinkRecordType, PaymentStatusType}
import refined_zone.pos_parbrink.processor.PaymentsProcessorTest.{
    OrderRaw,
    OrderTenderOnRead,
    OrderTenderRaw,
    PaymentRaw,
    PaymentRefined,
    PaymentsOnRead
}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class PaymentsProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val orderTender_1_onRead = OrderTenderOnRead(
        location_id = "loc_id_1",
        tender_id = 1,
        is_active_tender = true,
        tender_card_type = 0
    )
    val orderTender_2_onRead = OrderTenderOnRead(
        location_id = "loc_id_1",
        tender_id = 2,
        is_active_tender = false,
        tender_card_type = 3
    )

    val payment_1 =
        PaymentRaw(Id = 11, CardHolderName = "name_1   last_name", CardNumber = "1234", IsDeleted = false, TenderId = 1)
    val payment_2 =
        PaymentRaw(Id = 12, CardHolderName = "name_2/   last_name", CardNumber = null, IsDeleted = true, TenderId = 2)
    val payment_3 =
        PaymentRaw(Id = 13, CardHolderName = null, CardNumber = null, IsDeleted = false, TenderId = 3)

    val payments_1_onRead =
        PaymentsOnRead(
            location_id = "loc_id_1",
            order_id = 111,
            payments = Serialization.write(Seq(payment_1, payment_2))
        )
    val payments_2_onRead =
        PaymentsOnRead(location_id = "loc_id_1", order_id = 222, payments = Serialization.write(Seq(payment_3)))

    test("read and dedup Parbrink order tenders for payments processing") {

        // given

        val orderTender_1 = OrderTenderRaw(Id = 1, Active = true, CardType = 0)
        val orderTender_2 = OrderTenderRaw(Id = 2, Active = false, CardType = 3)
        val orderTender_3 = OrderTenderRaw(Id = 3, Active = true, CardType = 1)

        val rawData = Seq(
            ParbrinkRawZoneModel(
                record_value = Serialization.write(orderTender_1),
                record_type = ParbrinkRecordType.OrderTenders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/tenders_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel( // emulate duplicated data coming from different files
                record_value = Serialization.write(orderTender_1),
                record_type = ParbrinkRecordType.OrderTenders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/tenders_1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(orderTender_2),
                record_type = ParbrinkRecordType.OrderTenders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/tenders_2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = "orders", // not an orderTender type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(orderTender_3),
                record_type = ParbrinkRecordType.OrderTenders.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/tenders_1.json",
                cxi_id = "cxi_id_1"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val orderTendersOnRead = PaymentsProcessor.readOrderTenders(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink order tenders data does not match") {
            val expected = Seq(
                orderTender_1_onRead,
                orderTender_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, orderTendersOnRead.columns.size)
            assertDataFrameDataEquals(expected, orderTendersOnRead)
        }
    }

    test("read Parbrink payments") {

        // given
        val order_1 = OrderRaw(Id = 111, Payments = Seq(payment_1, payment_2))
        val order_2 = OrderRaw(Id = 222, Payments = Seq(payment_3))

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
                file_name = "location=loc_id_1/orders_11.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkRawZoneModel(
                record_value = Serialization.write(order_2),
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/orders_2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = ParbrinkRecordType.Categories.value, // not an order type, should be ignored
                location_id = "loc_id_1",
                feed_date = "2022-02-24",
                file_name = "location=loc_id_1/itemGroups_1.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkRawZoneModel(
                record_value = """{"key": "value"}""",
                record_type = ParbrinkRecordType.Orders.value,
                location_id = "loc_id_1",
                feed_date = "2022-05-05", // another feed date, should be ignored
                file_name = "location=loc_id_1/orders_2.json",
                cxi_id = "cxi_id_4"
            )
        ).toDF()
        val rawTable = "tempRawTable"
        rawData.createOrReplaceTempView(rawTable)

        // when
        val paymentsOnRead = PaymentsProcessor.readPayments(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink payments data does not match") {
            val expected = Seq(
                payments_1_onRead,
                payments_1_onRead, // emulate duplicated data coming from different files
                payments_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, paymentsOnRead.columns.size)
            assertDataFrameDataEquals(expected, paymentsOnRead)
        }
    }

    test("transform Parbrink payments") {

        // given
        val paymentsOnRead = Seq(
            payments_1_onRead,
            payments_1_onRead, // emulate duplicated data coming from different files
            payments_2_onRead
        ).toDF()

        val tenders = Seq(orderTender_1_onRead, orderTender_2_onRead).toDF()

        // when
        val transformedPayments = PaymentsProcessor.transformPayments(paymentsOnRead, tenders, cxiPartnerId)

        // then
        withClue("transformed Parbrink payments do not match") {
            val expected = Seq(
                PaymentRefined(
                    cxi_partner_id = cxiPartnerId,
                    payment_id = "111_11",
                    order_id = "111",
                    location_id = "loc_id_1",
                    status = PaymentStatusType.Active.value,
                    name = "name_1 last_name",
                    card_brand = CardBrandType.None.name,
                    pan = "1234"
                ),
                PaymentRefined(
                    cxi_partner_id = cxiPartnerId,
                    payment_id = "111_12",
                    order_id = "111",
                    location_id = "loc_id_1",
                    status = PaymentStatusType.Deleted.value,
                    name = "name_2/ last_name",
                    card_brand = CardBrandType.AmericanExpress.name,
                    pan = null
                ),
                PaymentRefined(
                    cxi_partner_id = cxiPartnerId,
                    payment_id = "222_13",
                    order_id = "222",
                    location_id = "loc_id_1",
                    status = null,
                    name = null,
                    card_brand = null,
                    pan = null
                )
            ).toDF()
            assert("Column size not Equal", expected.columns.size, transformedPayments.columns.size)
            assertDataFrameDataEquals(expected, transformedPayments)
        }
    }

}

object PaymentsProcessorTest {

    case class OrderTenderRaw(Id: Int, Active: Boolean, CardType: Int)

    case class OrderTenderOnRead(
        location_id: String,
        tender_id: Int,
        is_active_tender: Boolean,
        tender_card_type: Int
    )

    case class PaymentRaw(Id: Int, CardHolderName: String, CardNumber: String, IsDeleted: Boolean, TenderId: Int)
    case class OrderRaw(Id: Long, Payments: Seq[PaymentRaw])
    case class PaymentsOnRead(location_id: String, order_id: Long, payments: String)

    case class PaymentRefined(
        cxi_partner_id: String,
        payment_id: String,
        order_id: String,
        location_id: String,
        status: String,
        name: String,
        card_brand: String,
        pan: String,
        bin: String = null,
        exp_month: String = null,
        exp_year: String = null
    )
}

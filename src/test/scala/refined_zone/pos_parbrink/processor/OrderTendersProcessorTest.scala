package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.model.OrderTenderType
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.processor.OrderTendersProcessorTest.{
    OrderTenderOnRead,
    OrderTenderRaw,
    OrderTenderRefined
}
import support.BaseSparkBatchJobTest

import model.parbrink.ParbrinkRawZoneModel
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats

class OrderTendersProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    val orderTender_1_onRead = OrderTenderOnRead(
        location_id = "loc_id_1",
        tender_id = 1,
        tender_nm = "Cash",
        tender_type = 1
    )
    val orderTender_2_onRead = OrderTenderOnRead(
        location_id = "loc_id_1",
        tender_id = 2,
        tender_nm = "$1",
        tender_type = 4
    )

    test("read Parbrink order tenders") {

        // given

        val orderTender_1 = OrderTenderRaw(Id = 1, Name = "Cash", TenderType = 1)
        val orderTender_2 = OrderTenderRaw(Id = 2, Name = "$1", TenderType = 4)
        val orderTender_3 = OrderTenderRaw(Id = 3, Name = "House Account", TenderType = 6)

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
        val orderTendersOnRead = OrderTendersProcessor.readOrderTenders(spark, "2022-02-24", "false", rawTable)

        // then
        withClue("read Parbrink order tenders data does not match") {
            val expected = Seq(
                orderTender_1_onRead,
                orderTender_1_onRead, // emulate duplicated data coming from different files
                orderTender_2_onRead
            ).toDF()
            assert("Column size not Equal", expected.columns.size, orderTendersOnRead.columns.size)
            assertDataFrameDataEquals(expected, orderTendersOnRead)
        }
    }

    test("transform Parbrink order tenders") {

        // given
        val orderTendersOnRead = Seq(
            orderTender_1_onRead,
            orderTender_1_onRead, // emulate duplicated data coming from different files
            orderTender_2_onRead
        ).toDF()

        // when
        val transformedOrderTenders = OrderTendersProcessor.transformOrderTenders(orderTendersOnRead, cxiPartnerId)

        // then
        withClue("transformed Parbrink order tenders do not match") {
            val expected = Seq(
                OrderTenderRefined(
                    tender_id = 1,
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    tender_nm = "Cash",
                    tender_type = OrderTenderType.Cash.value
                ),
                OrderTenderRefined(
                    tender_id = 2,
                    cxi_partner_id = cxiPartnerId,
                    location_id = "loc_id_1",
                    tender_nm = "$1",
                    tender_type = OrderTenderType.GiftCard.value
                )
            ).toDF()
            assert("Column size not Equal", expected.columns.size, transformedOrderTenders.columns.size)
            assertDataFrameDataEquals(expected, transformedOrderTenders)
        }
    }

}

object OrderTendersProcessorTest {

    case class OrderTenderRaw(Id: Int, Name: String, TenderType: Int)
    case class OrderTenderOnRead(
        location_id: String,
        tender_id: Int,
        tender_nm: String,
        tender_type: Int
    )

    case class OrderTenderRefined(
        tender_id: Int,
        cxi_partner_id: String,
        location_id: String,
        tender_nm: String,
        tender_type: Int
    )
}

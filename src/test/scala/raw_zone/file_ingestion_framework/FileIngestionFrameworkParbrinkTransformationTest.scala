package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import raw_zone.file_ingestion_framework.FileIngestionFrameworkTransformations.transformParbrink
import support.BaseSparkBatchJobTest

import model.parbrink.{ParbrinkLandingZoneModel, ParbrinkRawZoneModel}
import model.parbrink.ParbrinkLandingZoneModel._
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.scalatest.Matchers

class FileIngestionFrameworkParbrinkTransformationTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    test("test Parbrink transformation") {

        // given
        val order_1 = Order(
            Id = 123,
            Entries = Array(
                OrderEntry(11, 10.34),
                OrderEntry(12, 2.0)
            ),
            Total = Some(12.34),
            IsClosed = true,
            Name = "ord_1",
            Payments = Array(
                OrderPayment(1, 7, "1234123412341234"),
                OrderPayment(2, 5.34, "4567456745674567")
            )
        )

        val order_2 = Order(
            Id = 234,
            Entries = Array(
                OrderEntry(22, 7.2)
            ),
            Total = None,
            IsClosed = false,
            Name = null,
            Payments = Array()
        )

        val order_3 = Order(
            Id = 567,
            Entries = Array(),
            Total = Some(99.99),
            IsClosed = false,
            Name = "ord_3",
            Payments = Array(
                OrderPayment(1, 42.42, "123456789012")
            )
        )

        val landingDf = Seq(
            ParbrinkLandingZoneModel( // 2 orders
                value = Serialization.write(Array(order_1, order_2)),
                feed_date = "2022-02-24",
                file_name = "location=loc1/orders_a1.json",
                cxi_id = "cxi_id_1"
            ),
            ParbrinkLandingZoneModel( // empty file
                value = Serialization.write(Array()),
                feed_date = "2022-02-24",
                file_name = "location=loc1/orders_a2.json",
                cxi_id = "cxi_id_2"
            ),
            ParbrinkLandingZoneModel( // 1 order, another location
                value = Serialization.write(Array(order_3)),
                feed_date = "2022-02-24",
                file_name = "location=loc2/orders_a3.json",
                cxi_id = "cxi_id_3"
            ),
            ParbrinkLandingZoneModel( // 3 tenders
                value = Serialization.write(
                    Array(
                        Tender(Id = 1, Active = true, Name = "tender_1"),
                        Tender(Id = 2, Active = true, Name = "tender_2"),
                        Tender(Id = 3, Active = false, Name = "tender_3")
                    )
                ),
                feed_date = "2022-02-24",
                file_name = "location=loc1/tenders_a1.json",
                cxi_id = "cxi_id_4"
            ),
            ParbrinkLandingZoneModel( // location options - not an array
                value = Serialization.write(
                    LocationOptions(Zip = "01234", Latitude = 33.025114, Longitude = -117.082445)
                ),
                feed_date = "2022-02-24",
                file_name = "location=loc1/options_a1.json",
                cxi_id = "cxi_id_5"
            )
        ).toDF()

        // when
        val actual = transformParbrink(landingDf)

        // then
        withClue("Transformed Parbrink data does not match") {
            val expected = Seq(
                ParbrinkRawZoneModel(
                    record_type = "orders",
                    record_value =
                        """{"Entries":[{"GrossSales":10.34,"ItemId":11},{"GrossSales":2.0,"ItemId":12}],"Id":123,
                          |"IsClosed":true,"Name":"ord_1","Payments":[{"Amount":7.0,"CardNumber":"1234123412341234",
                          |"Id":1},{"Amount":5.34,"CardNumber":"4567456745674567","Id":2}],"Total":12.34}""".stripMargin
                            .replace("\n", ""),
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/orders_a1.json",
                    cxi_id = "cxi_id_1"
                ),
                ParbrinkRawZoneModel(
                    record_type = "orders",
                    record_value =
                        """{"Entries":[{"GrossSales":7.2,"ItemId":22}],"Id":234,"IsClosed":false,"Payments":[]}""",
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/orders_a1.json",
                    cxi_id = "cxi_id_1"
                ),
                ParbrinkRawZoneModel(
                    record_type = "orders",
                    record_value = null,
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/orders_a2.json",
                    cxi_id = "cxi_id_2"
                ),
                ParbrinkRawZoneModel(
                    record_type = "orders",
                    record_value = """{"Entries":[],"Id":567,"IsClosed":false,"Name":"ord_3",
                          |"Payments":[{"Amount":42.42,"CardNumber":"123456789012","Id":1}],"Total":99.99}""".stripMargin
                        .replace("\n", ""),
                    location_id = "loc2",
                    feed_date = "2022-02-24",
                    file_name = "location=loc2/orders_a3.json",
                    cxi_id = "cxi_id_3"
                ),
                ParbrinkRawZoneModel(
                    record_type = "tenders",
                    record_value = """{"Active":true,"Id":1,"Name":"tender_1"}""",
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/tenders_a1.json",
                    cxi_id = "cxi_id_4"
                ),
                ParbrinkRawZoneModel(
                    record_type = "tenders",
                    record_value = """{"Active":true,"Id":2,"Name":"tender_2"}""",
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/tenders_a1.json",
                    cxi_id = "cxi_id_4"
                ),
                ParbrinkRawZoneModel(
                    record_type = "tenders",
                    record_value = """{"Active":false,"Id":3,"Name":"tender_3"}""",
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/tenders_a1.json",
                    cxi_id = "cxi_id_4"
                ),
                ParbrinkRawZoneModel(
                    record_type = "options",
                    record_value = """{"Latitude":33.025114,"Longitude":-117.082445,"Zip":"01234"}""",
                    location_id = "loc1",
                    feed_date = "2022-02-24",
                    file_name = "location=loc1/options_a1.json",
                    cxi_id = "cxi_id_5"
                )
            ).toDF
            assertDataFrameDataEquals(expected, actual)
        }

    }

}

package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.hub.model.OrderTenderType
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class OrderTendersProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("transform tender types") {

        // given
        val cxiPartnerId = "partner1"
        val orderTenderTypes = Seq(
            (
                """[{
                          "amount_money": {
                            "amount": 0,
                            "currency": "USD"
                          },
                          "cash_details": {
                            "buyer_tendered_money": {
                              "amount": 0,
                              "currency": "USD"
                            },
                            "change_back_money": {
                              "amount": 0,
                              "currency": "USD"
                            }
                          },
                          "created_at": "2021-12-27T19:31:36Z",
                          "id": "tender-1",
                          "location_id": "L0RPRF21MQRZH",
                          "processing_fee_money": {
                            "amount": 0,
                            "currency": "USD"
                          },
                          "transaction_id": "RSe4IDp7j3VtVyocDItvjopeV",
                          "type": "CASH"
                    }]""",
                "location-1"
            ),
            (
                """[{
                          "amount_money": {
                            "amount": 1161,
                            "currency": "USD"
                          },
                          "card_details": {
                            "card": {
                              "card_brand": "MASTERCARD",
                              "fingerprint": "sq-1-0XP3x0cTwxvmNE30M-R3Cfpignt1rNa4BWbpvSwhqsl-1vEv0kt0O8CFKrpq7xoOdA",
                              "last_4": "4223"
                            },
                            "entry_method": "EMV",
                            "status": "CAPTURED"
                          },
                          "created_at": "2021-12-28T02:24:56Z",
                          "customer_id": "1DFFENNWES5AS71QQSYKG2EAMW",
                          "id": "tender-2",
                          "location_id": "L0RPRF21MQRZH",
                          "processing_fee_money": {
                            "amount": 34,
                            "currency": "USD"
                          },
                          "transaction_id": "zxzTk6xrZHGvopuPTTWBR66eV",
                          "type": "SQUARE_GIFT_CARD"
                    }]""",
                "location-2"
            )
        ).toDF("tenders", "location_id")

        // when
        val actual = OrderTendersProcessor.transformOrderTenders(orderTenderTypes, cxiPartnerId)

        // then
        withClue("order tender types are not correctly transformed") {
            val expected = Seq(
                ("location-1", cxiPartnerId, "tender-1", null, OrderTenderType.Cash.code),
                ("location-2", cxiPartnerId, "tender-2", null, OrderTenderType.GiftCard.code)
            ).toDF("location_id", "cxi_partner_id", "tender_id", "tender_nm", "tender_type")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

}

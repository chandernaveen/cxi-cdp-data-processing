package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class PaymentsProcessorTest extends BaseSparkBatchJobTest {

    test("test toast partner payments read") {
        // given
        import spark.implicits._
        val payments = List(
            (
                s"""
                   |{
                   |   "approvalStatus":"NEEDS_APPROVAL",
                   |   "businessDate":20220311,
                   |   "guid": "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                   |   "checks":[
                   |      {
                   |         "amount":7248.64,
                   |         "entityType":"Check",
                   |         "guid":"2370f247-2ef9-4b2b-a074-6773c5a4b76a",
                   |         "openedDate":"2022-03-11T17:02:52.455+0000",
                   |         "paidDate":"2022-03-11T17:02:53.896+0000",
                   |         "paymentStatus":"CLOSED",
                   |         "payments":[
                   |            {
                   |               "amount":7991.8,
                   |               "amountTendered":7991.8,
                   |               "checkGuid":"2370f247-2ef9-4b2b-a074-6773c5a4b76a",
                   |               "entityType":"OrderPayment",
                   |               "guid":"9e963a26-0cd7-4ef1-882c-0826f4592f81",
                   |               "orderGuid":"ab73af06-2da2-4eb4-b1e4-b4733af8e4f3",
                   |               "otherPayment":{
                   |                  "entityType":"AlternatePaymentType",
                   |                  "guid":"cbff6ecc-0528-4dba-9779-683930de9987"
                   |               },
                   |               "paidBusinessDate":20220311,
                   |               "paidDate":"2022-03-11T17:02:50.000+0000",
                   |               "paymentStatus":"CAPTURED",
                   |               "refundStatus":"NONE",
                   |               "tipAmount":0.0,
                   |               "type":"OTHER",
                   |               "cardType": "VISA",
                   |               "last4Digits": "1234"
                   |            }
                   |         ]
                   |      }
                   |   ]
                   |}
                   |""".stripMargin,
                "f987f24a-2ee9-4550-8e76-1fad471c1136",
                "orders",
                "2022-02-24"
            ),
            (
                s"""
                   {
                       "approvalStatus":"APPROVED",
                       "businessDate":20220428,
                       "guid": "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                       "checks":[
                          {
                             "amount":14.74,
                             "closedDate":"2022-04-28T16:54:24.768+0000",
                             "createdDate":"2022-04-28T16:54:24.775+0000",
                             "customer":{
                                "guid":"fb2e2cc0-3788-43e7-8175-89044ae8bcbe"
                             },
                             "entityType":"Check",
                             "guid":"1d820fc7-14ff-4c78-b293-b46d8d687c34",
                             "openedDate":"2022-04-28T16:54:24.706+0000",
                             "paidDate":"2022-04-28T16:54:24.768+0000",
                             "paymentStatus":"CLOSED",
                             "payments":[
                                {
                                   "amount":16.25,
                                   "amountTendered":16.25,
                                   "checkGuid":"1d820fc7-14ff-4c78-b293-b46d8d687c34",
                                   "entityType":"OrderPayment",
                                   "guid":"138a90ec-22db-4216-ae55-f28fd4f3a313",
                                   "orderGuid":"4bf66bd8-2112-4808-a547-109e9fb62151",
                                   "otherPayment":{
                                      "entityType":"AlternatePaymentType",
                                      "guid":"a3ed4678-b2df-45c0-9b94-8e700b019620"
                                   },
                                   "paidBusinessDate":20220428,
                                   "paidDate":"2022-04-28T16:54:24.707+0000",
                                   "paymentStatus":"CAPTURED",
                                   "refundStatus":"NONE",
                                   "tipAmount":0.0,
                                   "type":"OTHER"
                                }
                             ]
                          }
                       ]
                    }
                   """,
                "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                "orders",
                "2022-02-24"
            ),
            (
                s"""
                   {
                       "approvalStatus":"NEEDS_APPROVAL",
                       "businessDate":20220311,
                       "checks":[],
                       "guid": "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9"
                   }
                   """,
                "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                "orders",
                "2021-10-10"
            ) // duplicate with diff feed date that gets filtered out
        ).toDF("record_value", "location_id", "record_type", "feed_date")

        val tableName = "payments"
        payments.createOrReplaceTempView(tableName)

        // when
        val actual = PaymentsProcessor.readPayments(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "order_id",
                "location_id",
                "payment_id",
                "status",
                "card_brand",
                "pan",
                "first_name",
                "last_name"
            )
        }
        val actualToastPaymentsData = actual.collect()
        withClue("POS Toast refined payments data do not match") {
            val expected = List(
                (
                    "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                    "f987f24a-2ee9-4550-8e76-1fad471c1136",
                    "9e963a26-0cd7-4ef1-882c-0826f4592f81",
                    "CAPTURED",
                    "VISA",
                    "1234",
                    null,
                    null
                ),
                (
                    "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                    "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                    "138a90ec-22db-4216-ae55-f28fd4f3a313",
                    "CAPTURED",
                    null,
                    null,
                    null,
                    null
                )
            ).toDF(
                "order_id",
                "location_id",
                "payment_id",
                "status",
                "card_brand",
                "pan",
                "first_name",
                "last_name"
            ).collect()
            actualToastPaymentsData.length should equal(expected.length)
            actualToastPaymentsData should contain theSameElementsAs expected
        }
    }

    test("test toast partner payments transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val payments = List(
            (
                "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                "f987f24a-2ee9-4550-8e76-1fad471c1136",
                "9e963a26-0cd7-4ef1-882c-0826f4592f81",
                "CAPTURED",
                "VISA",
                "1234",
                null,
                null
            ),
            (
                "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                "f987f24a-2ee9-4550-8e76-1fad471c1136",
                "9e963a26-0cd7-4ef1-882c-0826f4592f81",
                "CAPTURED",
                "VISA",
                "1234",
                null,
                null
            ), // duplicate
            (
                "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                "138a90ec-22db-4216-ae55-f28fd4f3a313",
                "CAPTURED",
                null,
                null,
                null,
                null
            )
        ).toDF(
            "order_id",
            "location_id",
            "payment_id",
            "status",
            "card_brand",
            "pan",
            "first_name",
            "last_name"
        )

        // when
        val actual = PaymentsProcessor.transformPayments(payments, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "order_id",
                "location_id",
                "payment_id",
                "status",
                "card_brand",
                "pan",
                "first_name",
                "last_name",
                "bin",
                "exp_month",
                "exp_year",
                "cxi_partner_id"
            )
        }
        val actualToastPaymentsData = actual.collect()
        withClue("POS Toast refined payments data do not match") {
            val expected = List(
                (
                    "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                    "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                    "138a90ec-22db-4216-ae55-f28fd4f3a313",
                    "CAPTURED",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    cxiPartnerId
                ),
                (
                    "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                    "f987f24a-2ee9-4550-8e76-1fad471c1136",
                    "9e963a26-0cd7-4ef1-882c-0826f4592f81",
                    "CAPTURED",
                    "VISA",
                    "1234",
                    null,
                    null,
                    null,
                    null,
                    null,
                    cxiPartnerId
                )
            ).toDF(
                "order_id",
                "location_id",
                "payment_id",
                "status",
                "card_brand",
                "pan",
                "first_name",
                "last_name",
                "bin",
                "exp_month",
                "exp_year",
                "cxi_partner_id"
            ).collect()
            actualToastPaymentsData should contain theSameElementsAs expected
        }
    }

}

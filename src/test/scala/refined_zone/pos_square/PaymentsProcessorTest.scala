package com.cxi.cdp.data_processing
package refined_zone.pos_square

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class PaymentsProcessorTest extends BaseSparkBatchJobTest {

    test("test square partner payments read") {
        // given
        import spark.implicits._
        val payments = List(
            (
                s"""
                   {
                       "card_details":{
                          "card":{
                             "bin":"371305",
                             "card_brand":"AMERICAN_EXPRESS",
                             "card_type":"CREDIT",
                             "cardholder_name":"VALUED CUSTOMER ",
                             "exp_month":8,
                             "exp_year":2024,
                             "fingerprint":"sq-1-vkCGXFBnTj6pP9akIt1-G9o4gmEruXABVurIoUsE7mGuGDH9sy0e-nFDstTNGX4Tag",
                             "last_4":"2002",
                             "prepaid_type":"NOT_PREPAID"
                          }
                       },
                       "id":"7Dh3CV1DNf6YXM3OFoUs32dkvaB",
                       "location_id":"L0P0DJ340FXF0",
                       "order_id":"yZJ4MfaYEXnvDFhZc8xrvTzeV",
                       "status":"COMPLETED"
                    }
                   """,
                "payments",
                "2021-10-11"
            ),
            (
                s"""
                   {
                     "id": "7Dh3CV1DNf6YXM3OFoUs32dkvaB"
                   }
                   """,
                "payments",
                "2021-10-10"
            ) // duplicate with diff date that gets filtered out
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "payments"
        payments.createOrReplaceTempView(tableName)

        // when
        val actual = PaymentsProcessor.readPayments(spark, "2021-10-11", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "payment_id",
                "order_id",
                "location_id",
                "status",
                "name",
                "card_brand",
                "pan",
                "bin",
                "exp_month",
                "exp_year"
            )
        }
        val actualSquarePaymentsData = actual.collect()
        withClue("POS Square refined payments data do not match") {
            val expected = List(
                (
                    "7Dh3CV1DNf6YXM3OFoUs32dkvaB",
                    "yZJ4MfaYEXnvDFhZc8xrvTzeV",
                    "L0P0DJ340FXF0",
                    "COMPLETED",
                    "VALUED CUSTOMER ",
                    "AMERICAN_EXPRESS",
                    "2002",
                    "371305",
                    "8",
                    "2024"
                )
            ).toDF(
                "payment_id",
                "order_id",
                "location_id",
                "status",
                "name",
                "card_brand",
                "pan",
                "bin",
                "exp_month",
                "exp_year"
            ).collect()
            actualSquarePaymentsData.length should equal(expected.length)
            actualSquarePaymentsData should contain theSameElementsAs expected
        }
    }

    test("test square partner payments transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val payments = List(
            (
                "7Dh3CV1DNf6YXM3OFoUs32dkvaB",
                "yZJ4MfaYEXnvDFhZc8xrvTzeV",
                "L0P0DJ340FXF0",
                "COMPLETED",
                "VALUED CUSTOMER ",
                "AMERICAN_EXPRESS",
                "2002",
                "371305",
                "8",
                "2024"
            ),
            (
                "7Dh3CV1DNf6YXM3OFoUs32dkvaB",
                "yZJ4MfaYEXnvDFhZc8xrvTzeV",
                "L0P0DJ340FXF0",
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ) // duplicate
        ).toDF(
            "payment_id",
            "order_id",
            "location_id",
            "status",
            "name",
            "card_brand",
            "pan",
            "bin",
            "exp_month",
            "exp_year"
        )

        // when
        val actual = PaymentsProcessor.transformPayments(payments, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "payment_id",
                "order_id",
                "location_id",
                "status",
                "name",
                "card_brand",
                "pan",
                "bin",
                "exp_month",
                "exp_year",
                "cxi_partner_id"
            )
        }
        val actualSquareCustomersData = actual.collect()
        withClue("POS Square refined payments data do not match") {
            val expected = List(
                (
                    "7Dh3CV1DNf6YXM3OFoUs32dkvaB",
                    "yZJ4MfaYEXnvDFhZc8xrvTzeV",
                    "L0P0DJ340FXF0",
                    "COMPLETED",
                    "VALUED CUSTOMER ",
                    "AMERICAN_EXPRESS",
                    "2002",
                    "371305",
                    "8",
                    "2024",
                    cxiPartnerId
                )
            ).toDF(
                "payment_id",
                "order_id",
                "location_id",
                "status",
                "name",
                "card_brand",
                "pan",
                "bin",
                "exp_month",
                "exp_year",
                "cxi_partner_id"
            ).collect()
            actualSquareCustomersData.length should equal(expected.length)
            actualSquareCustomersData should contain theSameElementsAs expected
        }
    }

}

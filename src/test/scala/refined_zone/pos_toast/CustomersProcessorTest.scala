package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class CustomersProcessorTest extends BaseSparkBatchJobTest {

    test("test readCustomers") {
        // given
        import spark.implicits._
        val orders = List(
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
                   |         "customer":{
                   |             "guid":"858c82f6-60b2-4bc2-8f9d-8c4c721aab50"
                   |         }
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
                                "guid":"fb2e2cc0-3788-43e7-8175-89044ae8bcbe",
                                "firstName": "John",
                                "lastName": "Doe",
                                "phone": "phone_hash",
                                "email": "email_hash"
                             },
                             "entityType":"Check",
                             "guid":"1d820fc7-14ff-4c78-b293-b46d8d687c34"
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
            ) // diff feed date that gets filtered out
        ).toDF("record_value", "location_id", "record_type", "feed_date")

        val tableName = "orders"
        orders.createOrReplaceTempView(tableName)

        // when
        val actual =
            CustomersProcessor.readCustomers(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name"
            )
        }
        withClue("POS Toast refined order customer data do not match") {
            val expected = List(
                ("858c82f6-60b2-4bc2-8f9d-8c4c721aab50", null, null, null, null),
                ("fb2e2cc0-3788-43e7-8175-89044ae8bcbe", "email_hash", "phone_hash", "John", "Doe")
            ).toDF(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name"
            )
            actual.collect() should contain theSameElementsAs expected.collect()

        }
    }

    test("testTransformCustomers") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val readCustomers = List(
            ("858c82f6-60b2-4bc2-8f9d-8c4c721aab50", null, null, null, null),
            ("858c82f6-60b2-4bc2-8f9d-8c4c721aab50", null, null, null, null), // duplicate
            ("fb2e2cc0-3788-43e7-8175-89044ae8bcbe", "email_hash", "phone_hash", "John", "Doe")
        ).toDF(
            "customer_id",
            "email_address",
            "phone_number",
            "first_name",
            "last_name"
        )

        // when
        val actual =
            CustomersProcessor.transformCustomers(readCustomers, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name",
                "cxi_partner_id",
                "created_at",
                "version"
            )
        }
        withClue("POS Toast customer data do not match") {
            val expected = List(
                ("858c82f6-60b2-4bc2-8f9d-8c4c721aab50", null, null, null, null, cxiPartnerId, null, null),
                (
                    "fb2e2cc0-3788-43e7-8175-89044ae8bcbe",
                    "email_hash",
                    "phone_hash",
                    "John",
                    "Doe",
                    cxiPartnerId,
                    null,
                    null
                )
            ).toDF(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name",
                "cxi_partner_id",
                "created_at",
                "version"
            )
            actual.collect() should contain theSameElementsAs expected.collect()

        }
    }

}

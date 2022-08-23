package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityType.{CombinationBin, Email, Phone}
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import com.cxi.cdp.data_processing.support.normalization.DateNormalization.parseToLocalDate
import refined_zone.hub.identity.model.IdentityType
import support.crypto_shredding.config.CryptoShreddingConfig
import support.utils.ContractUtils
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.time.LocalDate

class PosIdentityProcessorTest extends BaseSparkBatchJobTest {

    test("test readOrderCustomerDataWithAppliedPreauthInfoIfAvailable") {
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
                                "guid":"fb2e2cc0-3788-43e7-8175-89044ae8bcbe"
                             },
                             "entityType":"Check",
                             "guid":"1d820fc7-14ff-4c78-b293-b46d8d687c34",
                             "openedDate":"2022-04-28T16:54:24.706+0000",
                             "paidDate":"2022-04-28T16:54:24.768+0000",
                             "paymentStatus":"CLOSED",
                             "appliedPreauthInfo": {
                                 "cardType": "VISA",
                                 "last4CardDigits": "1234",
                                 "cardHolderFirstName": "John",
                                 "cardHolderLastName": "Doe",
                                 "cardHolderExpirationMonth": "06",
                                 "cardHolderExpirationYear": "25"
                             }
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
            PosIdentityProcessor.readOrderCustomerDataWithAppliedPreauthInfoIfAvailable(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "order_id",
                "location_id",
                "customer_id",
                "card_brand",
                "pan",
                "first_name",
                "last_name",
                "exp_month",
                "exp_year"
            )
        }
        withClue("POS Toast refined order customer data do not match") {
            val expected = List(
                (
                    "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                    "f987f24a-2ee9-4550-8e76-1fad471c1136",
                    "858c82f6-60b2-4bc2-8f9d-8c4c721aab50",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                (
                    "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                    "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                    "fb2e2cc0-3788-43e7-8175-89044ae8bcbe",
                    "VISA",
                    "1234",
                    "John",
                    "Doe",
                    "06",
                    "25"
                )
            ).toDF(
                "order_id",
                "location_id",
                "customer_id",
                "card_brand",
                "pan",
                "first_name",
                "last_name",
                "exp_month",
                "exp_year"
            )
            actual.collect() should contain theSameElementsAs expected.collect()

        }

    }

    test("test transformOrderCustomerDataWithAppliedPreauthInfoIfAvailable") {
        // given
        import spark.implicits._
        val customersPreAuth = List(
            (
                "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                "f987f24a-2ee9-4550-8e76-1fad471c1136",
                "858c82f6-60b2-4bc2-8f9d-8c4c721aab50",
                null,
                null,
                null,
                null,
                null,
                null
            ),
            (
                "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                "fb2e2cc0-3788-43e7-8175-89044ae8bcbe",
                "VISA",
                "1234",
                "John",
                "Doe",
                "06",
                "25"
            )
        ).toDF(
            "order_id",
            "location_id",
            "customer_id",
            "card_brand",
            "pan",
            "first_name",
            "last_name",
            "exp_month",
            "exp_year"
        )

        val payments = List(
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
                "cxi-usa-testpartner"
            ),
            (
                "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                "f987f24a-2ee9-4550-8e76-1fad471c1136",
                "9e963a26-0cd7-4ef1-882c-0826f4592f81",
                "CAPTURED",
                "VISA",
                "5678",
                null,
                null,
                null,
                null,
                null,
                "cxi-usa-testpartner"
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
        )

        // when
        val actual =
            PosIdentityProcessor.transformOrderCustomerDataWithAppliedPreauthInfoIfAvailable(customersPreAuth, payments)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "order_id",
                "location_id",
                "customer_id",
                "card_brand",
                "bin",
                "pan",
                "first_name",
                "last_name",
                "exp_month",
                "exp_year"
            )
        }
        withClue("POS Toast refined order customer data do not match") {
            val expected = List(
                (
                    "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                    "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                    "fb2e2cc0-3788-43e7-8175-89044ae8bcbe",
                    "VISA",
                    null,
                    "1234",
                    "John",
                    "Doe",
                    "06",
                    "25"
                ),
                (
                    "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                    "f987f24a-2ee9-4550-8e76-1fad471c1136",
                    "858c82f6-60b2-4bc2-8f9d-8c4c721aab50",
                    "VISA",
                    null,
                    "5678",
                    null,
                    null,
                    null,
                    null
                )
            ).toDF(
                "order_id",
                "location_id",
                "customer_id",
                "card_brand",
                "bin",
                "pan",
                "first_name",
                "last_name",
                "exp_month",
                "exp_year"
            )
            actual.collect() should contain theSameElementsAs expected.collect()

        }

    }

    test("test readCustomerDim") {
        import spark.implicits._
        // given
        val cxiPartnerId = "some-partner-id"
        val customersTable = "customers"
        val customers = List(
            ("cust1", "hash_email", "hash_phone", cxiPartnerId),
            ("cust2", null, "hash_phone", cxiPartnerId),
            ("cust3", "hash_email", null, cxiPartnerId),
            ("cust4", null, null, cxiPartnerId),
            ("cust5", null, null, "diff_partner")
        ).toDF("customer_id", "email_address", "phone_number", "cxi_partner_id")
        customers.createOrReplaceTempView(customersTable)

        // when
        val actual = PosIdentityProcessor.readCustomerDim(spark, customersTable, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("customer_id", "email_address", "phone_number")
        }
        withClue("POS Toast refined customer data do not match") {
            val expected = List(
                ("cust1", "hash_email", "hash_phone"),
                ("cust2", null, "hash_phone"),
                ("cust3", "hash_email", null)
            ).toDF(
                "customer_id",
                "email_address",
                "phone_number"
            )
            actual.collect() should contain theSameElementsAs expected.collect()

        }
    }

    test("test readPrivacyLookupTable") {
        import spark.implicits._
        // given
        val cxiPartnerId = "some-partner-id"
        val lookupTableName = "lookup_table"
        val lookupTable = List(
            ("johndoe@gmail.com", "hash_email", IdentityType.Email.code, "2022-02-24", "runId1", cxiPartnerId),
            (
                "johndoe2@gmail.com",
                "hash_email",
                IdentityType.Email.code,
                "2022-02-24",
                "runId2",
                cxiPartnerId
            ), // filtered out
            (
                "johndoe3@gmail.com",
                "hash_email",
                IdentityType.Email.code,
                "2022-02-25",
                "runId1",
                cxiPartnerId
            ), // filtered out
            (
                "johndoe4@gmail.com",
                "hash_email",
                IdentityType.Email.code,
                "2022-02-24",
                "runId1",
                "some-other-partner"
            ), // filtered out
            ("+380939876654", "phone_hash", IdentityType.Phone.code, "2022-02-24", "runId1", cxiPartnerId)
        ).toDF("original_value", "hashed_value", "identity_type", "feed_date", "run_id", "cxi_source")
        lookupTable.createOrReplaceGlobalTempView(lookupTableName)

        val cryptoShreddingConfig = CryptoShreddingConfig(
            cxiSource = cxiPartnerId,
            date = LocalDate.of(2022, 2, 24),
            runId = "runId1",
            lookupDestDbName = "foobar",
            lookupDestTableName = "foobaz",
            workspaceConfigPath = "foo"
        )
        val contract = new ContractUtils(s"""
               |{
               | "schema": {
               |   "crypto": {
               |     "db_name": "global_temp",
               |     "lookup_table": "lookup_table"
               |   }
               | }
               |}
               |""".stripMargin)

        // when
        val actual = PosIdentityProcessor.readPrivacyLookupTable(spark, contract, cryptoShreddingConfig)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("original_value", "hashed_value", "identity_type")
        }
        withClue("POS Toast read lookup data do not match") {
            val expected = List(
                ("johndoe@gmail.com", "hash_email", IdentityType.Email.code),
                ("+380939876654", "phone_hash", IdentityType.Phone.code)
            ).toDF("original_value", "hashed_value", "identity_type")
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("test findPosIdentitiesWeight3ForAnOrder") {
        import spark.implicits._
        // given
        val privacyTable = List(
            ("johndoe@gmail.com", "hash_email", IdentityType.Email.code),
            ("foobar@gmail.com", "hash_email2", IdentityType.Email.code),
            ("+380939876654", "phone_hash", IdentityType.Phone.code)
        ).toDF("original_value", "hashed_value", "identity_type")
        val customers = List(
            ("cust1", "hash_email", "hash_phone"),
            ("cust2", null, "hash_phone"),
            ("cust3", "hash_email2", null)
        ).toDF(
            "customer_id",
            "email_address",
            "phone_number"
        )
        val transformedOrderCustomerData = List(
            ("ord1", "loc1", "cust1"),
            ("ord2", "loc2", "cust2"),
            ("ord3", "loc1", "cust3"),
            ("ord4", "loc3", "cust4") // filtered out
        ).toDF("order_id", "location_id", "customer_id")

        // when
        val actual = PosIdentityProcessor.findPosIdentitiesWeight3ForAnOrder(
            transformedOrderCustomerData,
            customers,
            privacyTable
        )

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("order_id", "location_id", "cxi_identity_id", "type", "weight")
        }
        withClue("POS Toast findPosIdentitiesWeight3ForAnOrder data do not match") {
            val expected = List(
                ("ord1", "loc1", "hash_email", IdentityType.Email.code, 3),
                ("ord3", "loc1", "hash_email2", IdentityType.Email.code, 3)
            ).toDF("order_id", "location_id", "cxi_identity_id", "type", "weight")
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("test enrichPosIdentityWithMetadata") {

        import spark.implicits._

        // given
        val date = parseToLocalDate("2022-02-24")
        val allCustomersIds = Seq(
            ("ord_1", date, "loc_1", "email_hash_1", Email.value, "3"),
            ("ord_2", date, "loc_1", "email_hash_2", Email.value, "3"), // not exists in privacy table
            ("ord_3", date, "loc_1", "comb-bin_1", CombinationBin.value, "1"),
            ("ord_4", date, "loc_1", "phone_hash_1", Phone.value, "2")
        ).toDF("ord_id", "ord_timestamp", "ord_location_id", "cxi_identity_id", "type", "weight")

        val privacyTable = Seq(
            ("email_1@gmail.com", "email_hash_1", Email.value),
            ("1234567890", "phone_hash_1", Phone.value)
        ).toDF("original_value", "hashed_value", "identity_type")

        // when
        val actual = PosIdentityProcessor.enrichPosIdentityWithMetadata(privacyTable, allCustomersIds)

        // then
        withClue("Identities with metadata do not match") {
            val expected = Seq(
                ("email_hash_1", Email.value, "3", Map("email_domain" -> "gmail.com")),
                ("email_hash_2", Email.value, "3", Map.empty[String, String]),
                ("comb-bin_1", CombinationBin.value, "1", Map.empty[String, String]),
                ("phone_hash_1", Phone.value, "2", Map("phone_area_code" -> "1234"))
            ).toDF(CxiIdentityId, Type, Weight, Metadata)

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}

package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import refined_zone.hub.identity.model.IdentityType.CombinationCardSlim
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import support.crypto_shredding.config.CryptoShreddingConfig
import support.normalization.TimestampNormalization.parseToTimestamp
import support.utils.ContractUtils
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

import java.time.LocalDate

class PosIdentityProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("test readOrderPaymentData") {

        // given
        val tableName = "raw_tickets"
        val dateNow = "2022-07-28"
        val rawTickets = List(
            (
                "tickets",
                """
                {
                    "_embedded": {
                        "payments": [{
                            "amount": 5240,
                            "change": 0,
                            "id": "20",
                            "last4": null,
                            "status": null,
                            "tip": 0,
                            "type": "cash"
                        }]
                    },
                    "_links": {
                        "self": {
                            "href": "https://api.omnivore.io/1.0/locations/cRyzGpdi/tickets/14/",
                            "type": "application/hal+json; name=ticket"
                        }
                    },
                    "closed_at": 1657340725,
                    "id": "ordId_1",
                    "open": false,
                    "opened_at": 1657212092,
                    "void": false
                }
                """.stripMargin,
                dateNow,
                "file_name1",
                "cxi_id1"
            ),
            (
                "tickets",
                """
                {
                    "_embedded": {
                        "payments": []
                    },
                    "_links": {
                        "self": {
                            "href": "https://api.omnivore.io/1.0/locations/cRyzGpdi/tickets/14/",
                            "type": "application/hal+json; name=ticket"
                        }
                    },
                    "closed_at": 1757340725,
                    "id": "ordId_2",
                    "open": false,
                    "opened_at": 1757212092,
                    "void": false
                }
                """.stripMargin,
                dateNow,
                "file_name1",
                "cxi_id1"
            ),
            (
                "tickets",
                """
                {
                    "_embedded": {
                        "payments": [{
                            "amount": 5240,
                            "change": 0,
                            "id": "20",
                            "last4": null,
                            "status": null,
                            "tip": 0,
                            "type": "cash"
                        }]
                    },
                    "_links": {
                        "self": {
                            "href": "https://api.omnivore.io/1.0/locations/cRyzGpdi/tickets/14/",
                            "type": "application/hal+json; name=ticket"
                        }
                    },
                    "closed_at": null,
                    "id": "ordId_3",
                    "open": true,
                    "opened_at": 1657212092,
                    "void": false
                }
                """.stripMargin,
                dateNow,
                "file_name1",
                "cxi_id1"
            )
        ).toDF("record_type", "record_value", "feed_date", "file_name", "cxi_id")

        rawTickets.createOrReplaceGlobalTempView(tableName)

        // when
        val ticketsActual = PosIdentityProcessor.readOrderPaymentData(spark, dateNow, "global_temp", tableName)

        // then
        withClue("Actual fields returned:\n" + ticketsActual.schema.treeString) {
            val actualFieldsReturned = ticketsActual.schema.fields.map(f => f.name)
            actualFieldsReturned shouldEqual Array(
                "ord_id",
                "location_url",
                "payments"
            )
        }

        // Then
        withClue("Read order customer data do not match") {
            val actualOrderCustomerData = ticketsActual.collect
            val expected = List(
                (
                    "ordId_1",
                    "https://api.omnivore.io/1.0/locations/cRyzGpdi/tickets/14/",
                    """
                    [{
                        "amount": 5240,
                        "change": 0,
                        "id": "20",
                        "last4": null,
                        "status": null,
                        "tip": 0,
                        "type": "cash"
                    }]
                    """.replaceAll("\\s", "")
                )
            ).toDF(
                "ord_id",
                "location_url",
                "payments"
            ).collect()

            actualOrderCustomerData.length should equal(expected.length)
            actualOrderCustomerData should contain theSameElementsAs expected
        }
    }

    test("test transformOrderPayment") {

        // given
        val orderPaymentData = Seq(
            (
                "ord-id-1",
                "https://api.omnivore.io/1.0/locations/loc-id-1/tickets/14/",
                """
                [{
                    "_embedded": {
                        "tender_type": {
                            "allows_tips": false,
                            "id": "1",
                            "name": "VISA",
                            "pos_id": "101"
                        }
                    },
                    "amount": 5240,
                    "change": 0,
                    "full_name": null,
                    "id": "20",
                    "last4": 1234,
                    "status": null,
                    "tip": 0,
                    "type": "card"
                }]
                """.replaceAll("\\s", "")
            )
        ).toDF(
            "ord_id",
            "location_url",
            "payments"
        )

        // when
        val actual = PosIdentityProcessor.transformOrderPayment("partner-id-1", orderPaymentData)

        // then
        withClue("Transformed data do not match") {

            val expectedData = Seq(
                Row(
                    "ord-id-1",
                    "loc-id-1",
                    "partner-id-1",
                    "VISA",
                    "1234",
                    ""
                )
            )

            val orderCustomerSchema = DataTypes.createStructType(
                Array(
                    StructField("ord_id", StringType),
                    StructField("location_id", StringType),
                    StructField("cxi_partner_id", StringType, nullable = false),
                    StructField("tender_type", StringType),
                    StructField("ord_payment_last4", StringType),
                    StructField("ord_customer_full_name", StringType)
                )
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expectedData.asJava, orderCustomerSchema)

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("test readPrivacyLookupTable") {

        // given
        val cxiPartnerId = "partner-id-1"
        val dateNow = "2022-07-28"
        val lookupTableName = "lookup_table"
        val lookupTable = List(
            ("pks-visa-1234", "card-hash-1", CombinationCardSlim.code, dateNow, "run-id-1", cxiPartnerId),
            ("-visa-6789", "card-hash-2", CombinationCardSlim.code, dateNow, "run-id-1", cxiPartnerId),
            ("abc-visa-4567", "card-hash-3", CombinationCardSlim.code, dateNow, "run-id-2", cxiPartnerId),
            ("abc-visa-4568", "card-hash-4", CombinationCardSlim.code, "2022-07-25", "run-id-1", cxiPartnerId),
            ("abc-visa-4569", "card-hash-5", CombinationCardSlim.code, dateNow, "run-id-1", "partner-id-2")
        ).toDF("original_value", "hashed_value", "identity_type", "feed_date", "run_id", "cxi_source")
        lookupTable.createOrReplaceGlobalTempView(lookupTableName)

        val cryptoShreddingConfig = CryptoShreddingConfig(
            cxiSource = cxiPartnerId,
            date = LocalDate.of(2022, 7, 28),
            runId = "run-id-1",
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
        withClue("POS Omnivore read lookup data do not match") {
            val expected = List(
                ("pks-visa-1234", "card-hash-1", CombinationCardSlim.code),
                ("-visa-6789", "card-hash-2", CombinationCardSlim.code)
            ).toDF("original_value", "hashed_value", "identity_type")
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("test enrichPosIdentityWithMetadata") {

        // given
        val posIdentities = Seq(
            ("ord_1", "comb-card-slim_1", CombinationCardSlim.code, "2")
        ).toDF("ord_id", "cxi_identity_id", "type", "weight")

        val privacyTable = Seq(
            ("pks-visa-1234", "comb-card-slim_1", CombinationCardSlim.code)
        ).toDF("original_value", "hashed_value", "identity_type")

        // when
        val actual = PosIdentityProcessor.enrichPosIdentityWithMetadata(privacyTable, posIdentities)

        // then
        withClue("Identities with metadata do not match") {
            val expected = Seq(
                ("comb-card-slim_1", CombinationCardSlim.value, "2", Map.empty[String, String])
            ).toDF(CxiIdentityId, Type, Weight, Metadata)

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}

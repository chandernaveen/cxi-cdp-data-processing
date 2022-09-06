package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.pos_parbrink.model.{CardBrandType, PaymentStatusType}
import refined_zone.pos_parbrink.processor.identity.PaymentsIdentitiesProcessor.createCardHolderNameTypeNumberIdentity
import refined_zone.pos_parbrink.processor.identity.PaymentsIdentitiesProcessorTest.CardHolderNameTypeNumberTestCase
import refined_zone.pos_parbrink.processor.PaymentsProcessorTest.PaymentRefined
import support.crypto_shredding.hashing.Hash
import support.utils.crypto_shredding.CryptoShreddingTransformOnly
import support.BaseSparkBatchJobTest

import org.json4s.DefaultFormats
import org.scalatest.Matchers.convertToAnyShouldWrapper

class PaymentsIdentitiesProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    implicit val formats: DefaultFormats = DefaultFormats

    val cxiPartnerId = "cxi-usa-partner-1"

    test("create card holder name + card type + card number identity") {

        // given
        val testCases = Seq(
            CardHolderNameTypeNumberTestCase(
                "GARCIA/RICHARD",
                "MasterCard",
                "9621",
                Some("garcia/richard-mastercard-9621")
            ),
            CardHolderNameTypeNumberTestCase(
                "   GARCIA  /   RICHARD    ",
                "  MasterCard    ",
                "   9621    ",
                Some("garcia/richard-mastercard-9621")
            ),
            CardHolderNameTypeNumberTestCase(
                "",
                "MasterCard",
                "9621",
                None
            ),
            CardHolderNameTypeNumberTestCase(
                null,
                "MasterCard",
                "9621",
                None
            ),
            CardHolderNameTypeNumberTestCase(
                "GARCIA/RICHARD",
                null,
                "9621",
                None
            ),
            CardHolderNameTypeNumberTestCase(
                "GARCIA/RICHARD",
                "MasterCard",
                " ",
                None
            ),
            CardHolderNameTypeNumberTestCase(
                "/RICHARD",
                "MasterCard",
                "9621",
                None
            ),
            CardHolderNameTypeNumberTestCase(
                "GARCIARICHARD",
                "MasterCard",
                "9621",
                None
            ),
            CardHolderNameTypeNumberTestCase(
                "GARCIA RICHARD /",
                "MasterCard",
                "9621",
                None
            )
        )

        // when
        for (testCase <- testCases) {
            createCardHolderNameTypeNumberIdentity(
                testCase.holderName,
                testCase.cardBrand,
                testCase.pan
                // then
            ) shouldEqual testCase.identity

        }
    }

    test("compute identities from payment details") {
        // given
        val payments = Seq(
            PaymentRefined(
                cxi_partner_id = cxiPartnerId,
                payment_id = "111_11",
                order_id = "111",
                location_id = "loc_id_1",
                status = PaymentStatusType.Active.value,
                name = "GARCIA/RICHARD",
                card_brand = CardBrandType.Visa.name,
                pan = "1234"
            ),
            PaymentRefined(
                cxi_partner_id = cxiPartnerId,
                payment_id = "111_12",
                order_id = "111",
                location_id = "loc_id_1",
                status = PaymentStatusType.Active.value,
                name = "some_name",
                card_brand = CardBrandType.AmericanExpress.name,
                pan = "2345"
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

        // when
        val cryptoShredding = new CryptoShreddingTransformOnly()
        val actual = PaymentsIdentitiesProcessor.computeIdentitiesFromPayments(payments, cryptoShredding)

        // then
        withClue("identities computed from payment details do not match") {
            val expected = Seq(
                (
                    "111",
                    "loc_id_1",
                    Hash.sha256Hash("garcia/richard-visa-1234", cryptoShredding.SaltTest),
                    IdentityType.CardHolderNameTypeNumber.code,
                    2
                )
            ).toDF("order_id", "location_id", "cxi_identity_id", "type", "weight")

            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }

    }

}

object PaymentsIdentitiesProcessorTest {

    case class CardHolderNameTypeNumberTestCase(
        holderName: String,
        cardBrand: String,
        pan: String,
        identity: Option[String]
    )

}

package com.cxi.cdp.data_processing
package support.normalization

import refined_zone.hub.model.OrderTenderType
import refined_zone.pos_square.model.PosSquareOrderTenderTypes.PosSquareToCxiTenderType
import support.normalization.OrderTenderTypeNormalization.normalizeOrderTenderType

import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

class CxiTaxonomyNormalizationTest extends FunSuite {

    test("Normalize order tender type") {

        val testCases = Seq(
            TenderTypeNormalizationTestCase("CASH", OrderTenderType.Cash),
            TenderTypeNormalizationTestCase("Cash", OrderTenderType.Cash),
            TenderTypeNormalizationTestCase("WALLET", OrderTenderType.Wallet),
            TenderTypeNormalizationTestCase("Wallet", OrderTenderType.Wallet),
            TenderTypeNormalizationTestCase("OTHER", OrderTenderType.Other),
            TenderTypeNormalizationTestCase("other", OrderTenderType.Other),
            TenderTypeNormalizationTestCase("CARD", OrderTenderType.CreditCard),
            TenderTypeNormalizationTestCase("CarD", OrderTenderType.CreditCard),
            TenderTypeNormalizationTestCase("NO_SALE", OrderTenderType.Other),
            TenderTypeNormalizationTestCase("no_sale", OrderTenderType.Other),
            TenderTypeNormalizationTestCase("SQUARE_GIFT_CARD", OrderTenderType.GiftCard),
            TenderTypeNormalizationTestCase("square_gift_card", OrderTenderType.GiftCard),
            TenderTypeNormalizationTestCase("", OrderTenderType.Unknown),
            TenderTypeNormalizationTestCase("something", OrderTenderType.Other),
            TenderTypeNormalizationTestCase(null, OrderTenderType.Unknown)
        )

        for (testCase <- testCases) {
            normalizeOrderTenderType(testCase.value, PosSquareToCxiTenderType) shouldBe testCase.expected
        }

    }

    case class TenderTypeNormalizationTestCase(value: String, expected: OrderTenderType)

}

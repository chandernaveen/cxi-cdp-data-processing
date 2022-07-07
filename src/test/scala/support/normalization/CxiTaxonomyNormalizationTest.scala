package com.cxi.cdp.data_processing
package support.normalization

import refined_zone.hub.model.{OrderStateType, OrderTenderType}
import refined_zone.pos_square.model.PosSquareOrderStateTypes.PosSquareToCxiOrderStateType
import refined_zone.pos_square.model.PosSquareOrderTenderTypes.PosSquareToCxiTenderType
import support.normalization.OrderStateNormalization.normalizeOrderState
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

    test("Normalize order state type") {

        val testCases = Seq(
            OrderStateTypeNormalizationTestCase("Open", OrderStateType.Open),
            OrderStateTypeNormalizationTestCase("OPEN", OrderStateType.Open),
            OrderStateTypeNormalizationTestCase("draft", OrderStateType.Draft),
            OrderStateTypeNormalizationTestCase("DRAFT", OrderStateType.Draft),
            OrderStateTypeNormalizationTestCase("completeD", OrderStateType.Completed),
            OrderStateTypeNormalizationTestCase("COMPLETED", OrderStateType.Completed),
            OrderStateTypeNormalizationTestCase("canceLled", OrderStateType.Cancelled),
            OrderStateTypeNormalizationTestCase("CANCELLED", OrderStateType.Cancelled),
            OrderStateTypeNormalizationTestCase("", OrderStateType.Unknown),
            OrderStateTypeNormalizationTestCase("something", OrderStateType.Other),
            OrderStateTypeNormalizationTestCase(null, OrderStateType.Unknown)
        )

        for (testCase <- testCases) {
            normalizeOrderState(testCase.value, PosSquareToCxiOrderStateType) shouldBe testCase.expected
        }

    }

    case class TenderTypeNormalizationTestCase(value: String, expected: OrderTenderType)
    case class OrderStateTypeNormalizationTestCase(value: String, expected: OrderStateType)

}

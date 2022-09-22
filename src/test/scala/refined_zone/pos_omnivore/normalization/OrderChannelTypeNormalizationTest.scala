package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore.normalization

import raw_zone.pos_omnivore.model.OrderType
import refined_zone.hub.model.OrderChannelType

import org.scalatest.{FunSuite, Matchers}

class OrderChannelTypeNormalizationTest extends FunSuite with Matchers {

    import OrderChannelTypeNormalization._

    test("Omnivore.normalizeOrderOriginateChannelType") {
        val testCases = Seq[(Option[OrderType], OrderChannelType)](
            None -> OrderChannelType.Unknown,
            Some(OrderType(name = null)) -> OrderChannelType.Unknown,
            Some(OrderType(name = "")) -> OrderChannelType.Unknown,
            Some(OrderType(name = "Dine In")) -> OrderChannelType.PhysicalLane,
            Some(OrderType(name = "Eat In")) -> OrderChannelType.PhysicalLane,
            Some(OrderType(name = "Carry Out")) -> OrderChannelType.PhysicalLane,
            Some(OrderType(name = "Delivery")) -> OrderChannelType.Other,
            Some(OrderType(name = "Order Ahead")) -> OrderChannelType.Other
        )

        testCases.foreach { case (orderType, expectedOrderChannelType) =>
            withClue((orderType, expectedOrderChannelType)) {
                normalizeOrderOriginateChannelType(orderType) shouldBe expectedOrderChannelType
            }
        }
    }

    test("Omnivore.normalizeOrderTargetChannelType") {
        val testCases = Seq[(Option[OrderType], OrderChannelType)](
            None -> OrderChannelType.Unknown,
            Some(OrderType(name = null)) -> OrderChannelType.Unknown,
            Some(OrderType(name = "")) -> OrderChannelType.Unknown,
            Some(OrderType(name = "Dine In")) -> OrderChannelType.PhysicalLane,
            Some(OrderType(name = "Eat In")) -> OrderChannelType.PhysicalLane,
            Some(OrderType(name = "Carry Out")) -> OrderChannelType.PhysicalPickup,
            Some(OrderType(name = "Delivery")) -> OrderChannelType.PhysicalDelivery,
            Some(OrderType(name = "Order Ahead")) -> OrderChannelType.PhysicalLane
        )

        testCases.foreach { case (orderType, expectedOrderChannelType) =>
            withClue((orderType, expectedOrderChannelType)) {
                normalizeOrderTargetChannelType(orderType) shouldBe expectedOrderChannelType
            }
        }
    }

}

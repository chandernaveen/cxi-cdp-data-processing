package com.cxi.cdp.data_processing
package refined_zone.pos_square.normalization

import raw_zone.pos_square.model.{Fulfillment, OrderSource, PickupDetails}
import raw_zone.pos_square.model.Fulfillment.{State, Type}
import refined_zone.hub.model.OrderChannelType

import org.scalatest.{FunSuite, Matchers}

class OrderChannelTypeNormalizationTest extends FunSuite with Matchers {

    import OrderChannelTypeNormalization._

    test("Square.normalizeOrderOriginateChannelType") {
        val testCases = Seq[(Option[OrderSource], OrderChannelType)](
            None -> OrderChannelType.PhysicalLane,
            Some(OrderSource(name = null)) -> OrderChannelType.PhysicalLane,
            Some(OrderSource(name = "")) -> OrderChannelType.PhysicalLane,
            Some(OrderSource(name = "Popmenu")) -> OrderChannelType.DigitalWeb,
            Some(OrderSource(name = "Popmenu 123")) -> OrderChannelType.DigitalWeb,
            Some(OrderSource(name = "Doordash")) -> OrderChannelType.DigitalApp,
            Some(OrderSource(name = "Doordash 456")) -> OrderChannelType.DigitalApp,
            Some(OrderSource(name = "Ubereats")) -> OrderChannelType.DigitalApp,
            Some(OrderSource(name = "Ubereats 7")) -> OrderChannelType.DigitalApp,
            Some(OrderSource(name = "Grubhubweb")) -> OrderChannelType.DigitalApp,
            Some(OrderSource(name = "Grubhubweb 890")) -> OrderChannelType.DigitalApp,
            Some(OrderSource(name = "Square Online")) -> OrderChannelType.DigitalWeb,
            Some(OrderSource(name = "Square Online 111")) -> OrderChannelType.DigitalWeb,
            Some(OrderSource(name = "Unknown name")) -> OrderChannelType.PhysicalLane
        )

        testCases.foreach { case (orderSource, expectedOrderChannelType) =>
            withClue((orderSource, expectedOrderChannelType)) {
                normalizeOrderOriginateChannelType(orderSource) shouldBe expectedOrderChannelType
            }
        }
    }

    test("Square.orderTargetChannelTypeFromOrderSource") {
        val testCases = Seq[(OrderSource, Option[OrderChannelType])](
            OrderSource(name = null) -> None,
            OrderSource(name = "") -> None,
            OrderSource(name = "Popmenu") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Popmenu 123") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Doordash") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Doordash 456") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Ubereats") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Ubereats 7") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Grubhubweb") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Grubhubweb 890") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Square Online") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Square Online 111") -> Some(OrderChannelType.PhysicalDelivery),
            OrderSource(name = "Unknown name") -> None
        )

        testCases.foreach { case (orderSource, expectedOrderChannelType) =>
            withClue((orderSource, expectedOrderChannelType)) {
                orderTargetChannelTypeFromOrderSource(orderSource) shouldBe expectedOrderChannelType
            }
        }
    }

    test("Square.orderTargetChannelTypeFromFulfillments") {
        val protoFulfillment = Fulfillment(pickup_details = PickupDetails(note = "a note"), state = "FL")
        val unknownType = "some unknown type"

        val testCases = Seq[(Seq[Fulfillment], Option[OrderChannelType])](
            Seq.empty -> None,
            Seq(protoFulfillment.copy(`type` = Type.Pickup)) -> Some(OrderChannelType.PhysicalPickup),
            Seq(protoFulfillment.copy(`type` = unknownType)) -> None,

            // prefer COMPLETED fulfillment
            Seq(
                protoFulfillment.copy(`type` = unknownType, state = State.Proposed),
                protoFulfillment.copy(`type` = Type.Pickup, state = State.Completed)
            ) -> Some(OrderChannelType.PhysicalPickup),

            // COMPLETED fulfillment has null type, so ignore it
            Seq(
                protoFulfillment.copy(`type` = null, state = State.Completed),
                protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)
            ) -> Some(OrderChannelType.PhysicalPickup),

            // COMPLETED fulfillment has an unknown type
            Seq(
                protoFulfillment.copy(`type` = unknownType, state = State.Completed),
                protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)
            ) -> None
        )

        testCases.foreach { case (fulfillments, expectedOrderChannelType) =>
            withClue((fulfillments, expectedOrderChannelType)) {
                orderTargetChannelTypeFromFulfillments(fulfillments) shouldBe expectedOrderChannelType
            }
        }
    }

    test("Square.normalizeOrderTargetChannelType") {
        val physicalDeliveryOrderSource = OrderSource(name = "Ubereats")
        val unknownOrderSource = OrderSource(name = "Unknown order source")

        val protoFulfillment = Fulfillment(pickup_details = PickupDetails(note = "a note"), state = "FL")
        val pickupFulfillments = Seq(protoFulfillment.copy(`type` = Type.Pickup, state = State.Completed))
        val unknownFulfillments = Seq(protoFulfillment.copy(`type` = "some unknown type", state = State.Completed))

        val testCases = Seq[((Option[OrderSource], Option[Seq[Fulfillment]]), OrderChannelType)](
            (None, None) -> OrderChannelType.Unknown,
            (None, Some(Seq.empty)) -> OrderChannelType.Unknown,
            (Some(unknownOrderSource), Some(unknownFulfillments)) -> OrderChannelType.Unknown,
            (Some(unknownOrderSource), None) -> OrderChannelType.Unknown,
            (None, Some(unknownFulfillments)) -> OrderChannelType.Unknown,

            // derive order target channel type from orderSource first
            (Some(physicalDeliveryOrderSource), None) -> OrderChannelType.PhysicalDelivery,
            (Some(physicalDeliveryOrderSource), Some(pickupFulfillments)) -> OrderChannelType.PhysicalDelivery,

            // derive order target channel type from fulfillments if it cannot be derived from order source
            (None, Some(pickupFulfillments)) -> OrderChannelType.PhysicalPickup,
            (Some(unknownOrderSource), Some(pickupFulfillments)) -> OrderChannelType.PhysicalPickup
        )

        testCases.foreach { case ((maybeOrderSource, maybeFulfillments), expectedOrderChannelType) =>
            withClue(((maybeOrderSource, maybeFulfillments), expectedOrderChannelType)) {
                normalizeOrderTargetChannelType(
                    maybeOrderSource,
                    maybeFulfillments
                ) shouldBe expectedOrderChannelType
            }
        }
    }

}

package com.cxi.cdp.data_processing
package refined_zone.pos_square

import com.cxi.cdp.data_processing.model.ChannelType
import com.cxi.cdp.data_processing.raw_zone.pos_square.model.{Fulfillment, PickupDetails}
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest
import org.apache.spark.sql.functions.col
import org.scalatest.Matchers

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {

    import OrderSummaryProcessorTest._

    test("getOrdTargetChannelId") {
        import Fulfillment.{State, Type}
        import spark.implicits._

        val protoFulfillment = Fulfillment(pickup_details = PickupDetails(note = "a note"), state = "FL")
        val unknownType = "some unknown type"

        val inputDF = List[OrdTargetChannelIdTestCase](
            OrdTargetChannelIdTestCase(
                fulfillments = null,
                expectedOrdTargetChannelId = ChannelType.Unknown.code),

            OrdTargetChannelIdTestCase(
                fulfillments = Seq(),
                expectedOrdTargetChannelId = ChannelType.Unknown.code),

            OrdTargetChannelIdTestCase(
                fulfillments = Seq(protoFulfillment.copy(`type` = Type.Pickup)),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code),

            // prefer COMPLETED fulfillment
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = unknownType, state = State.Proposed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Completed)),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code),

            // COMPLETED fulfillment has null type, so ignore it
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = null, state = State.Completed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code),

            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = unknownType, state = State.Completed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)),
                expectedOrdTargetChannelId = ChannelType.Unknown.code),

            OrdTargetChannelIdTestCase(
                fulfillments = Seq(protoFulfillment.copy(`type` = unknownType)),
                expectedOrdTargetChannelId = ChannelType.Unknown.code))
            .toDS

        val results = inputDF
            .withColumn("actualOrdTargetChannelId", OrderSummaryProcessor.getOrdTargetChannelId(col("fulfillments")))
            .collect

        results.foreach { result =>
            val fulfillments = result.getAs[Seq[Fulfillment]]("fulfillments")
            val expectedOrdTargetChannelId = result.getAs[Int]("expectedOrdTargetChannelId")
            val actualOrdTargetChannelId = result.getAs[Int]("actualOrdTargetChannelId")
            withClue(s"actual channel type id does not match for fulfillments: $fulfillments") {
                actualOrdTargetChannelId shouldBe expectedOrdTargetChannelId
            }
        }
    }

}

object OrderSummaryProcessorTest {
    case class OrdTargetChannelIdTestCase(fulfillments: Seq[Fulfillment], expectedOrdTargetChannelId: Int)
}

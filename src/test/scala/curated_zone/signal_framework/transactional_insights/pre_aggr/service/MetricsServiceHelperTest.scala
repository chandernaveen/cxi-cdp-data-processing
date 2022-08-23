package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import curated_zone.model.signal.transactional_insights.{ChannelMetric, TenderTypeMetric}
import refined_zone.hub.model.{OrderChannelType, OrderTenderType}
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers

class MetricsServiceHelperTest extends BaseSparkBatchJobTest with Matchers {

    import MetricsServiceHelper._

    test("channelMetric") {
        val validMapping = Map(
            (OrderChannelType.PhysicalLane, OrderChannelType.Unknown) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.PhysicalKiosk, OrderChannelType.Unknown) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.PhysicalLane, OrderChannelType.Other) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.PhysicalKiosk, OrderChannelType.Other) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.Unknown, OrderChannelType.PhysicalLane) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.Unknown, OrderChannelType.PhysicalKiosk) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.Unknown, OrderChannelType.PhysicalPickup) -> ChannelMetric.PhysicalPickup,
            (OrderChannelType.Unknown, OrderChannelType.PhysicalDelivery) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.Other, OrderChannelType.PhysicalLane) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.Other, OrderChannelType.PhysicalKiosk) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.Other, OrderChannelType.PhysicalPickup) -> ChannelMetric.PhysicalPickup,
            (OrderChannelType.Other, OrderChannelType.PhysicalDelivery) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.PhysicalLane, OrderChannelType.PhysicalLane) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.PhysicalLane, OrderChannelType.PhysicalKiosk) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.PhysicalLane, OrderChannelType.PhysicalPickup) -> ChannelMetric.PhysicalPickup,
            (OrderChannelType.PhysicalLane, OrderChannelType.PhysicalDelivery) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.PhysicalKiosk, OrderChannelType.PhysicalLane) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.PhysicalKiosk, OrderChannelType.PhysicalKiosk) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.PhysicalKiosk, OrderChannelType.PhysicalPickup) -> ChannelMetric.PhysicalPickup,
            (OrderChannelType.PhysicalKiosk, OrderChannelType.PhysicalDelivery) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.DigitalWeb, OrderChannelType.PhysicalLane) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.DigitalWeb, OrderChannelType.PhysicalKiosk) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.DigitalWeb, OrderChannelType.PhysicalPickup) -> ChannelMetric.PhysicalPickup,
            (OrderChannelType.DigitalWeb, OrderChannelType.PhysicalDelivery) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.DigitalApp, OrderChannelType.PhysicalLane) -> ChannelMetric.PhysicalLane,
            (OrderChannelType.DigitalApp, OrderChannelType.PhysicalKiosk) -> ChannelMetric.PhysicalKiosk,
            (OrderChannelType.DigitalApp, OrderChannelType.PhysicalPickup) -> ChannelMetric.PhysicalPickup,
            (OrderChannelType.DigitalApp, OrderChannelType.PhysicalDelivery) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.DigitalWeb, OrderChannelType.Unknown) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.DigitalWeb, OrderChannelType.Other) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.DigitalApp, OrderChannelType.Unknown) -> ChannelMetric.PhysicalDelivery,
            (OrderChannelType.DigitalApp, OrderChannelType.Other) -> ChannelMetric.PhysicalDelivery
        )

        for {
            ordOriginateChannelType <- OrderChannelType.values
            ordTargetChannelType <- OrderChannelType.values
        } {
            val expectedChannelMetric =
                validMapping.getOrElse((ordOriginateChannelType, ordTargetChannelType), ChannelMetric.Unknown)

            withClue((ordOriginateChannelType, ordTargetChannelType, expectedChannelMetric)) {
                channelMetric(ordOriginateChannelType, ordTargetChannelType) shouldBe expectedChannelMetric
            }
        }
    }

    test("channelMetricUdf") {
        // given
        import spark.implicits._
        import OrderChannelType._

        val invalidOrderChannelTypeCode = -42

        val testCases = List(
            // these test cases result in Unknown signal name as input order channel types are either missing or invalid
            (None, None, ChannelMetric.Unknown.signalName),
            (Some(PhysicalLane.code), None, ChannelMetric.Unknown.signalName),
            (None, Some(PhysicalLane.code), ChannelMetric.Unknown.signalName),
            (Some(PhysicalLane.code), Some(invalidOrderChannelTypeCode), ChannelMetric.Unknown.signalName),
            (Some(invalidOrderChannelTypeCode), Some(PhysicalLane.code), ChannelMetric.Unknown.signalName),
            // check a few test cases when order channel types are valid; all cases are covered in a channelMetric test
            (Some(PhysicalLane.code), Some(Other.code), ChannelMetric.PhysicalLane.signalName),
            (Some(Unknown.code), Some(PhysicalKiosk.code), ChannelMetric.PhysicalKiosk.signalName),
            (Some(DigitalApp.code), Some(PhysicalKiosk.code), ChannelMetric.PhysicalKiosk.signalName),
            (Some(DigitalWeb.code), Some(Unknown.code), ChannelMetric.PhysicalDelivery.signalName)
        ).toDF("ord_originate_channel_id", "ord_target_channel_id", "expected_channel_metric")

        val input = testCases.select("ord_originate_channel_id", "ord_target_channel_id")

        // when
        val actual = input.withColumn(
            "channel_metric",
            channelMetricUdf(col("ord_originate_channel_id"), col("ord_target_channel_id"))
        )

        // then
        val expected = testCases.withColumnRenamed("expected_channel_metric", "channel_metric")
        assertDataFrameNoOrderEquals(expected, actual)
    }

    test("dollarsToCentsUdf") {
        // given
        import spark.implicits._

        val expected = List(
            (Some(10.54), Some(1054L)),
            (Some(12.0), Some(1200L)),
            (None, None)
        ).toDF("dollars", "cents")

        val input = expected.select("dollars")

        // when
        val actual = input.withColumn("cents", dollarsToCentsUdf(col("dollars")))

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("extractTenderTypeMetric") {
        val testCases = Seq(
            OrderTenderType.GiftCard.code -> Some(TenderTypeMetric.GiftCard),
            OrderTenderType.CreditCard.code -> Some(TenderTypeMetric.Card),
            OrderTenderType.Cash.code -> Some(TenderTypeMetric.Cash),
            OrderTenderType.Wallet.code -> Some(TenderTypeMetric.Wallet),
            OrderTenderType.Other.code -> None,
            OrderTenderType.Unknown.code -> None,
            12345 -> None // this should never happen as all tender types are stored in normalized form
        )

        testCases.foreach { case (tenderType, expectedResult) =>
            extractTenderTypeMetric(tenderType) shouldBe expectedResult
        }
    }

}

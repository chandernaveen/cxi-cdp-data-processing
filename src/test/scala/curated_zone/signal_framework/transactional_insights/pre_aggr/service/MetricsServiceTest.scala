package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import refined_zone.hub.model.OrderTenderType
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers

import java.sql.Timestamp
import java.time.ZoneOffset

class MetricsServiceTest extends BaseSparkBatchJobTest with Matchers {

    import MetricsService._
    import MetricsServiceHelper._

    test("addOrderMetrics") {
        // given
        import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.OrderMetric._
        import spark.implicits._

        val expected = List(
            ("1", Some(10.54), 1L, Some(1054L)),
            ("2", Some(12.0), 1L, Some(1200L)),
            ("3", None, 1L, None),
            ("4", Some(0.0), 1L, Some(0L))
        ).toDF(
            "ord_id",
            "ord_pay_total",
            metricColumnName(signalDomainName, TotalOrders.signalName),
            metricColumnName(signalDomainName, TotalAmount.signalName)
        )

        val orderSummary = expected.select("ord_id", "ord_pay_total")

        // when
        val actual = addOrderMetrics(orderSummary)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("addTimeOfDayMetrics") {
        // given
        import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.TimeOfDayMetric._
        import spark.implicits._

        def timestampWithHourMinute(hour: Int, minute: Int): Timestamp = {
            val localDateTime = java.time.LocalDateTime.of(2022, java.time.Month.of(3), 10, hour, minute, 0)
            Timestamp.from(localDateTime.toInstant(ZoneOffset.UTC))
        }

        val expected = List(
            ("1", Some(timestampWithHourMinute(5, 2)), 1L, 0L, 0L, 0L, 0L, 0L),
            ("2", Some(timestampWithHourMinute(10, 25)), 0L, 1L, 0L, 0L, 0L, 0L),
            ("3", Some(timestampWithHourMinute(12, 0)), 0L, 0L, 1L, 0L, 0L, 0L),
            ("4", Some(timestampWithHourMinute(19, 59)), 0L, 0L, 0L, 1L, 0L, 0L),
            ("5", Some(timestampWithHourMinute(23, 11)), 0L, 0L, 0L, 0L, 1L, 0L),
            ("6", Some(timestampWithHourMinute(2, 0)), 0L, 0L, 0L, 0L, 0L, 1L),
            ("7", None, 0L, 0L, 0L, 0L, 0L, 0L)
        ).toDF(
            "ord_id",
            "ord_timestamp",
            metricColumnName(signalDomainName, EarlyMorning.signalName),
            metricColumnName(signalDomainName, LateMorning.signalName),
            metricColumnName(signalDomainName, EarlyAfternoon.signalName),
            metricColumnName(signalDomainName, LateAfternoon.signalName),
            metricColumnName(signalDomainName, EarlyNight.signalName),
            metricColumnName(signalDomainName, LateNight.signalName)
        )

        val orderSummary = expected.select("ord_id", "ord_timestamp")

        // when
        val actual = addTimeOfDayMetrics(orderSummary)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("addChannelMetrics") {
        // given
        import spark.implicits._
        import com.cxi.cdp.data_processing.refined_zone.hub.model.OrderChannelType._
        import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.ChannelMetric

        val expected = List(
            ("0", None, None, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            ("0", Some(Unknown.code), Some(Other.code), 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            ("1", Some(Unknown.code), Some(PhysicalLane.code), 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L),
            ("2", Some(Other.code), Some(PhysicalKiosk.code), 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L),
            ("3", Some(Unknown.code), Some(PhysicalPickup.code), 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L),
            ("4", Some(Other.code), Some(PhysicalDelivery.code), 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L)
        ).toDF(
            "ord_id",
            "ord_originate_channel_id",
            "ord_target_channel_id",
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.Unknown.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.PhysicalLane.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.PhysicalKiosk.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.PhysicalPickup.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.PhysicalDelivery.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.DigitalWeb.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.DigitalApp.signalName),
            metricColumnName(ChannelMetric.signalDomainName, ChannelMetric.Other.signalName)
        )

        val orderSummary = expected.select("ord_id", "ord_originate_channel_id", "ord_target_channel_id")

        // when
        val actual = addChannelMetrics(orderSummary)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("addTenderTypeMetrics") {
        // given
        import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.TenderTypeMetric._
        import spark.implicits._

        val expected = List(
            ("1", Seq("t1", "t2", "t3", "t4", "t5", "t6"), 3L, 2L, 1L, 0L),
            ("2", Seq("t7"), 0L, 0L, 0L, 1L),
            ("3", Seq("t8"), 0L, 0L, 0L, 0L),
            ("4", Seq("t9_missing"), 0L, 0L, 0L, 0L),
            ("5", Seq.empty, 0L, 0L, 0L, 0L)
        ).toDF(
            "ord_id",
            "tender_ids",
            metricColumnName(signalDomainName, Card.signalName),
            metricColumnName(signalDomainName, Cash.signalName),
            metricColumnName(signalDomainName, GiftCard.signalName),
            metricColumnName(signalDomainName, Wallet.signalName)
        )

        val orderSummary = expected.select("ord_id", "tender_ids")

        val orderTenderType = List(
            ("t1", OrderTenderType.CreditCard.code),
            ("t2", OrderTenderType.Cash.code),
            ("t3", OrderTenderType.GiftCard.code),
            ("t4", OrderTenderType.CreditCard.code),
            ("t5", OrderTenderType.Cash.code),
            ("t6", OrderTenderType.CreditCard.code),
            ("t7", OrderTenderType.Wallet.code),
            ("t8", 12345) // some unknown tender type
        ).toDF("tender_id", "tender_type")

        // when
        val actual = addTenderTypeMetrics(orderSummary, orderTenderType)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

}

package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import java.sql.Timestamp
import java.time.ZoneOffset

import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest
import org.scalatest.Matchers

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
            ("4", Some(0.0), 1L, Some(0L)),
        ).toDF(
            "ord_id",
            "ord_pay_total",
            metricColumnName(signalDomainName, TotalOrders.signalName),
            metricColumnName(signalDomainName, TotalAmount.signalName))

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
            metricColumnName(signalDomainName, LateNight.signalName))

        val orderSummary = expected.select("ord_id", "ord_timestamp")

        // when
        val actual = addTimeOfDayMetrics(orderSummary)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("addChannelMetrics") {
        // given
        import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.ChannelMetric._
        import spark.implicits._

        val expected = List(
            ("0", Some(0), 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            ("1", Some(1), 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L),
            ("2", Some(2), 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L),
            ("3", Some(3), 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L),
            ("4", Some(4), 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L),
            ("5", Some(5), 0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L),
            ("6", Some(6), 0L, 0L, 0L, 0L, 0L, 0L, 1L, 0L),
            ("7", Some(99), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L),
            ("8", Some(-42), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
            ("9", None, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
        ).toDF(
            "ord_id",
            "ord_target_channel_id",
            metricColumnName(signalDomainName, Unknown.signalName),
            metricColumnName(signalDomainName, PhysicalLane.signalName),
            metricColumnName(signalDomainName, PhysicalKiosk.signalName),
            metricColumnName(signalDomainName, PhysicalPickup.signalName),
            metricColumnName(signalDomainName, PhysicalDelivery.signalName),
            metricColumnName(signalDomainName, DigitalWeb.signalName),
            metricColumnName(signalDomainName, DigitalApp.signalName),
            metricColumnName(signalDomainName, Other.signalName))

        val orderSummary = expected.select("ord_id", "ord_target_channel_id")

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
            metricColumnName(signalDomainName, Wallet.signalName))

        val orderSummary = expected.select("ord_id", "tender_ids")

        val orderTenderType = List(
            ("t1", "CARD"),
            ("t2", "CASH"),
            ("t3", "SQUARE_GIFT_CARD"),
            ("t4", "CARD"),
            ("t5", "CASH"),
            ("t6", "CARD"),
            ("t7", "WALLET"),
            ("t8", "SOME_UNKNOWN_TENDER_TYPE")
        ).toDF("tender_id", "tender_type")

        // when
        val actual = addTenderTypeMetrics(orderSummary, orderTenderType)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

}

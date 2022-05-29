package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.TenderTypeMetric
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers

class MetricsServiceHelperTest extends BaseSparkBatchJobTest with Matchers {

    import MetricsServiceHelper._

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
            "GIFT_CARD" -> Some(TenderTypeMetric.GiftCard),
            "SQUARE_GIFT_CARD" -> Some(TenderTypeMetric.GiftCard),
            "CARD" -> Some(TenderTypeMetric.Card),
            "CASH" -> Some(TenderTypeMetric.Cash),
            "WALLET" -> Some(TenderTypeMetric.Wallet),
            "BOTTLE_CAPS" -> None
        )

        testCases.foreach { case (tenderType, expectedResult) =>
            extractTenderTypeMetric(tenderType) shouldBe expectedResult
        }
    }

}

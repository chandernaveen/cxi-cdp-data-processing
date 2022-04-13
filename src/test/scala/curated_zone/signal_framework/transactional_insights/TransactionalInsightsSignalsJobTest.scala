package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class TransactionalInsightsSignalsJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("Collect signals for specific time period") {

        // given
        //  7 days: 2022-02-18 ... 2022-02-24
        // 30 days: 2022-01-26 ... 2022-02-24
        // 60 days: 2021-12-27 ... 2022-02-24
        // 90 days: 2021-11-27 ... 2022-02-24
        val feedDate = "2022-02-24"

        val preAggTable = "temp_preagg"

        val preAggDf = Seq(
            // cust-1, partner-1, loc-1: order_metrics/total_orders metrics for all time periods
            // 7 days
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-02-24"), "order_metrics", "total_orders", 1),
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-02-18"), "order_metrics", "total_orders", 2),
            // 30 days
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-02-17"), "order_metrics", "total_orders", 3),
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-01-26"), "order_metrics", "total_orders", 4),
            // 60 days
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-01-25"), "order_metrics", "total_orders", 5),
            ("cust-1", "partner-1", "loc-1", sqlDate("2021-12-27"), "order_metrics", "total_orders", 6),
            // 90 days
            ("cust-1", "partner-1", "loc-1", sqlDate("2021-12-26"), "order_metrics", "total_orders", 7),
            ("cust-1", "partner-1", "loc-1", sqlDate("2021-11-27"), "order_metrics", "total_orders", 8),
            // out of the timeframes, should not be included into calculation
            ("cust-1", "partner-1", "loc-1", sqlDate("2021-11-26"), "order_metrics", "total_orders", 100),
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-02-25"), "order_metrics", "total_orders", 100),

            // cust-1, partner-2, loc-2: preagg metrics for 7 days period only
            // 7 days
            ("cust-1", "partner-2", "loc-2", sqlDate("2022-02-21"), "order_metrics", "total_orders", 9),
            ("cust-1", "partner-2", "loc-2", sqlDate("2022-02-21"), "order_metrics", "total_orders", 10),

            // cust-1, partner-1, loc-1: order_metrics/total_amount metrics for 60 days period only
            // 7 days
            ("cust-1", "partner-1", "loc-1", sqlDate("2021-12-27"), "order_metrics", "total_amount", 123),
            ("cust-1", "partner-1", "loc-1", sqlDate("2022-01-25"), "order_metrics", "total_amount", 34),

            // cust-2, partner-1, _ALL loc: order_metrics/total_amount metrics for 90 days period only
            // 7 days
            ("cust-1", "partner-1", "_ALL", sqlDate("2021-12-02"), "order_metrics", "total_amount", 456),
            ("cust-1", "partner-1", "_ALL", sqlDate("2021-12-03"), "order_metrics", "total_amount", 12),


        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_date", "signal_domain", "signal_name", "signal_value")

        preAggDf.createOrReplaceTempView(preAggTable)

        // when
        val actual = TransactionalInsightsSignalsJob.process(spark, feedDate, preAggTable)

        // then
        withClue("Transactional insights signals do not match") {
            val expected = Seq(
                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_orders", 3, "time_period_7"),
                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_orders", 10, "time_period_30"),
                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_orders", 21, "time_period_60"),
                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_orders", 36, "time_period_90"),

                ("cust-1", "partner-2", "loc-2", "order_metrics", "total_orders", 19, "time_period_7"),
                ("cust-1", "partner-2", "loc-2", "order_metrics", "total_orders", 19, "time_period_30"),
                ("cust-1", "partner-2", "loc-2", "order_metrics", "total_orders", 19, "time_period_60"),
                ("cust-1", "partner-2", "loc-2", "order_metrics", "total_orders", 19, "time_period_90"),

                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_amount", 157, "time_period_60"),
                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_amount", 157, "time_period_90"),

                ("cust-1", "partner-1", "_ALL", "order_metrics", "total_amount", 468, "time_period_90")

            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}

package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import org.scalatest.{FunSuite, Matchers}

class TimeOfDayMetricTest extends FunSuite with Matchers {

    test("fromHour should parse TimeOfDayMetric from hour") {
        val testCases = Seq(
            1 -> TimeOfDayMetric.LateNight,
            2 -> TimeOfDayMetric.LateNight,
            3 -> TimeOfDayMetric.LateNight,
            4 -> TimeOfDayMetric.LateNight,
            5 -> TimeOfDayMetric.EarlyMorning,
            6 -> TimeOfDayMetric.EarlyMorning,
            7 -> TimeOfDayMetric.EarlyMorning,
            8 -> TimeOfDayMetric.EarlyMorning,
            9 -> TimeOfDayMetric.LateMorning,
            10 -> TimeOfDayMetric.LateMorning,
            11 -> TimeOfDayMetric.LateMorning,
            12 -> TimeOfDayMetric.EarlyAfternoon,
            13 -> TimeOfDayMetric.EarlyAfternoon,
            14 -> TimeOfDayMetric.EarlyAfternoon,
            15 -> TimeOfDayMetric.EarlyAfternoon,
            16 -> TimeOfDayMetric.LateAfternoon,
            17 -> TimeOfDayMetric.LateAfternoon,
            18 -> TimeOfDayMetric.LateAfternoon,
            19 -> TimeOfDayMetric.LateAfternoon,
            20 -> TimeOfDayMetric.EarlyNight,
            21 -> TimeOfDayMetric.EarlyNight,
            22 -> TimeOfDayMetric.EarlyNight,
            23 -> TimeOfDayMetric.EarlyNight
        )

        testCases.foreach { case (hour, expectedTimeOfDay) =>
            TimeOfDayMetric.fromHour(hour) shouldBe expectedTimeOfDay
        }
    }

}

package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import org.scalatest.{FunSuite, Matchers}

class HourOfDayMetricTest extends FunSuite with Matchers {

    test("fromHour should parse HourOfDayMetric from hour") {
        val testCases = Seq(
            0 -> HourOfDayMetric.H00_01,
            1 -> HourOfDayMetric.H01_02,
            2 -> HourOfDayMetric.H02_03,
            3 -> HourOfDayMetric.H03_04,
            4 -> HourOfDayMetric.H04_05,
            5 -> HourOfDayMetric.H05_06,
            6 -> HourOfDayMetric.H06_07,
            7 -> HourOfDayMetric.H07_08,
            8 -> HourOfDayMetric.H08_09,
            9 -> HourOfDayMetric.H09_10,
            10 -> HourOfDayMetric.H10_11,
            11 -> HourOfDayMetric.H11_12,
            12 -> HourOfDayMetric.H12_13,
            13 -> HourOfDayMetric.H13_14,
            14 -> HourOfDayMetric.H14_15,
            15 -> HourOfDayMetric.H15_16,
            16 -> HourOfDayMetric.H16_17,
            17 -> HourOfDayMetric.H17_18,
            18 -> HourOfDayMetric.H18_19,
            19 -> HourOfDayMetric.H19_20,
            20 -> HourOfDayMetric.H20_21,
            21 -> HourOfDayMetric.H21_22,
            22 -> HourOfDayMetric.H22_23,
            23 -> HourOfDayMetric.H23_00
        )

        testCases.foreach { case (hour, expectedTimeOfDay) =>
            HourOfDayMetric.fromHour(hour) shouldBe expectedTimeOfDay
        }
    }

}

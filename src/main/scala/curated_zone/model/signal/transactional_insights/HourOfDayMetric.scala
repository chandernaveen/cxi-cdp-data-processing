// scalastyle:off number.of.types

package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.curated_zone.model.signal.{Signal, SignalDomain}

import enumeratum.values._

sealed abstract class HourOfDayMetric(val value: String) extends StringEnumEntry with Signal with Serializable {
    override def signalName: String = value
}

object HourOfDayMetric extends StringEnum[HourOfDayMetric] with SignalDomain[HourOfDayMetric] {

    override val signalDomainName = "hour_of_day_metrics"

    case object H00_01 extends HourOfDayMetric("00_01")
    case object H01_02 extends HourOfDayMetric("01_02")
    case object H02_03 extends HourOfDayMetric("02_03")
    case object H03_04 extends HourOfDayMetric("03_04")
    case object H04_05 extends HourOfDayMetric("04_05")
    case object H05_06 extends HourOfDayMetric("05_06")
    case object H06_07 extends HourOfDayMetric("06_07")
    case object H07_08 extends HourOfDayMetric("07_08")
    case object H08_09 extends HourOfDayMetric("08_09")
    case object H09_10 extends HourOfDayMetric("09_10")
    case object H10_11 extends HourOfDayMetric("10_11")
    case object H11_12 extends HourOfDayMetric("11_12")
    case object H12_13 extends HourOfDayMetric("12_13")
    case object H13_14 extends HourOfDayMetric("13_14")
    case object H14_15 extends HourOfDayMetric("14_15")
    case object H15_16 extends HourOfDayMetric("15_16")
    case object H16_17 extends HourOfDayMetric("16_17")
    case object H17_18 extends HourOfDayMetric("17_18")
    case object H18_19 extends HourOfDayMetric("18_19")
    case object H19_20 extends HourOfDayMetric("19_20")
    case object H20_21 extends HourOfDayMetric("20_21")
    case object H21_22 extends HourOfDayMetric("21_22")
    case object H22_23 extends HourOfDayMetric("22_23")
    case object H23_00 extends HourOfDayMetric("23_00")

    override val values = findValues
    override val signals: Seq[HourOfDayMetric] = values

    // scalastyle:off magic.number
    // scalastyle:off cyclomatic.complexity
    def fromHour(hour: Int): HourOfDayMetric = {
        hour match {
            case 0 => H00_01
            case 1 => H01_02
            case 2 => H02_03
            case 3 => H03_04
            case 4 => H04_05
            case 5 => H05_06
            case 6 => H06_07
            case 7 => H07_08
            case 8 => H08_09
            case 9 => H09_10
            case 10 => H10_11
            case 11 => H11_12
            case 12 => H12_13
            case 13 => H13_14
            case 14 => H14_15
            case 15 => H15_16
            case 16 => H16_17
            case 17 => H17_18
            case 18 => H18_19
            case 19 => H19_20
            case 20 => H20_21
            case 21 => H21_22
            case 22 => H22_23
            case 23 => H23_00
            case _ => throw new Exception("Invalid hour")
        }
    }
}

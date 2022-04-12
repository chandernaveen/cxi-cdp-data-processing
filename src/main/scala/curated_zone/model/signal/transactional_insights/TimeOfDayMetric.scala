package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.curated_zone.model.signal.{Signal, SignalDomain}
import enumeratum.values._

sealed abstract class TimeOfDayMetric(val value: String) extends StringEnumEntry with Signal with Serializable {
    override def signalName: String = value
}

object TimeOfDayMetric extends StringEnum[TimeOfDayMetric] with SignalDomain[TimeOfDayMetric] {

    override val signalDomainName = "time_of_day_metrics"

    case object EarlyMorning extends TimeOfDayMetric("early_morning")
    case object LateMorning extends TimeOfDayMetric("late_morning")
    case object EarlyAfternoon extends TimeOfDayMetric("early_afternoon")
    case object LateAfternoon extends TimeOfDayMetric("late_afternoon")
    case object EarlyNight extends TimeOfDayMetric("early_night")
    case object LateNight extends TimeOfDayMetric("late_night")

    override val values = findValues
    override val signals: Seq[TimeOfDayMetric] = values

    def fromHour(hour: Int): TimeOfDayMetric = {
        if (5 <= hour && hour < 9) {
            EarlyMorning
        } else if (9 <= hour && hour < 12) {
            LateMorning
        } else if (12 <= hour && hour < 16) {
            EarlyAfternoon
        } else if (16 <= hour && hour < 20) {
            LateAfternoon
        } else if (hour >= 20) {
            EarlyNight
        } else {
            LateNight
        }
    }

}

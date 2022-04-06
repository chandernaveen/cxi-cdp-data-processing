package com.cxi.cdp.data_processing
package curated_zone.model

import enumeratum.values._

import scala.collection.immutable

sealed abstract class CustomerMetricsTimePeriod(val value: String, val numberOfDays: Int) extends StringEnumEntry with Serializable

// scalastyle:off magic.number
object CustomerMetricsTimePeriod extends StringEnum[CustomerMetricsTimePeriod] {

    case object Period7days extends CustomerMetricsTimePeriod("time_period_7", 7)
    case object Period30days extends CustomerMetricsTimePeriod("time_period_30", 30)
    case object Period60days extends CustomerMetricsTimePeriod("time_period_60", 60)
    case object Period90days extends CustomerMetricsTimePeriod("time_period_90", 90)

    def values: immutable.IndexedSeq[CustomerMetricsTimePeriod] = findValues
}

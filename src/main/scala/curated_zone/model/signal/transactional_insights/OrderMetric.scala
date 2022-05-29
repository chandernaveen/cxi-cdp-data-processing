package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.curated_zone.model.signal.{Signal, SignalDomain}

import enumeratum.values._

sealed abstract class OrderMetric(val value: String) extends StringEnumEntry with Signal with Serializable {
    override def signalName: String = value
}

object OrderMetric extends StringEnum[OrderMetric] with SignalDomain[OrderMetric] {

    override val signalDomainName = "order_metrics"

    case object TotalOrders extends OrderMetric("total_orders")
    case object TotalAmount extends OrderMetric("total_amount")

    override val values = findValues
    override val signals: Seq[OrderMetric] = values

}

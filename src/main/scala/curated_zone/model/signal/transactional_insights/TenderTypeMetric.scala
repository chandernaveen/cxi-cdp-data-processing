package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.curated_zone.model.signal.{Signal, SignalDomain}
import enumeratum.values._

sealed abstract class TenderTypeMetric(val value: String) extends StringEnumEntry with Signal with Serializable {
    override def signalName: String = value
}

object TenderTypeMetric extends StringEnum[TenderTypeMetric] with SignalDomain[TenderTypeMetric] {

    override val signalDomainName = "tender_type_metrics"

    case object Card extends TenderTypeMetric("card")
    case object Cash extends TenderTypeMetric("cash")
    case object GiftCard extends TenderTypeMetric("gift_card")
    case object Wallet extends TenderTypeMetric("wallet")

    override val values = findValues
    override val signals: Seq[TenderTypeMetric] = values

}

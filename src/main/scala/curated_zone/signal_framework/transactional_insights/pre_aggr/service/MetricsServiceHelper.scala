package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights._
import com.cxi.cdp.data_processing.curated_zone.model.signal.SignalDomain

import org.apache.spark.sql.functions.udf

private[pre_aggr] object MetricsServiceHelper {

    val dollarsToCentsUdf = udf((maybeDollars: Option[Double]) => {
        maybeDollars.map(dollars => (dollars * 100).toLong)
    })

    val hourToTimeOfDayUdf = udf((maybeHour: Option[Int]) => {
        maybeHour.map(hour => TimeOfDayMetric.fromHour(hour).signalName)
    })

    val channelCodeToChannelMetricUdf = udf((maybeChannelTypeCode: Option[Int]) => {
        for {
            channelTypeCode <- maybeChannelTypeCode
            channelMetric <- ChannelMetric.fromChannelTypeCode(channelTypeCode)
        } yield channelMetric.signalName
    })

    /** Creates tender type metric from a String tender type value.
      *
      * Tender type values seem to be POS-specific, which can be seen in the parsing logic of GiftCard.
      * At the moment of writing this tender type was only available for Square, the following values were present:
      * CARD, CASH, NO_SALE, OTHER, WALLET, SQUARE_GIFT_CARD
      */
    def extractTenderTypeMetric(tenderType: String): Option[TenderTypeMetric] = {
        tenderType.toLowerCase match {
            case tenderTypeLc if tenderTypeLc.endsWith("gift_card") => Some(TenderTypeMetric.GiftCard)
            case "card" => Some(TenderTypeMetric.Card)
            case "cash" => Some(TenderTypeMetric.Cash)
            case "wallet" => Some(TenderTypeMetric.Wallet)
            case _ => None
        }
    }

    val extractTenderTypeMetricUdf = udf((maybeTenderType: Option[String]) => {
        maybeTenderType.flatMap(extractTenderTypeMetric).map(_.signalName)
    })

    final val MetricColumnSeparator = "__SEP__"

    def metricColumnName(signalDomain: String, signalName: String): String = {
        signalDomain + MetricColumnSeparator + signalName
    }

    def getMetricColumns(signalDomains: SignalDomain[_]*): Seq[String] = {
        for {
            signalDomain <- signalDomains.toSeq
            signalName <- signalDomain.signalNames
        } yield metricColumnName(signalDomain.signalDomainName, signalName)
    }

}

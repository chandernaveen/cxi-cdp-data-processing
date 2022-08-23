package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import curated_zone.model.signal.transactional_insights._
import curated_zone.model.signal.SignalDomain
import refined_zone.hub.model.{OrderChannelType, OrderTenderType}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf

private[pre_aggr] object MetricsServiceHelper {

    private val logger = Logger.getLogger(this.getClass.getName)

    val dollarsToCentsUdf = udf((maybeDollars: Option[Double]) => {
        maybeDollars.map(dollars => (dollars * 100).toLong)
    })

    val hourToTimeOfDayUdf = udf((maybeHour: Option[Int]) => {
        maybeHour.map(hour => TimeOfDayMetric.fromHour(hour).signalName)
    })

    def channelMetric(
        ordOriginateChannelType: OrderChannelType,
        ordTargetChannelType: OrderChannelType
    ): ChannelMetric = {
        import OrderChannelType._

        (ordOriginateChannelType, ordTargetChannelType) match {

            case (res @ (PhysicalLane | PhysicalKiosk), Unknown | Other) =>
                ChannelMetric.fromChannelType(res)

            case (
                    Unknown | Other | PhysicalLane | PhysicalKiosk | DigitalWeb | DigitalApp,
                    res @ (PhysicalLane | PhysicalKiosk | PhysicalPickup | PhysicalDelivery)
                ) =>
                ChannelMetric.fromChannelType(res)

            case (DigitalWeb | DigitalApp, Unknown | Other) =>
                ChannelMetric.PhysicalDelivery

            case _ => ChannelMetric.Unknown

        }
    }

    val channelMetricUdf = udf((maybeOrdOriginateChannelId: Option[Int], maybeOrdTargetChannelId: Option[Int]) => {
        val maybeChannelMetric = for {
            ordOriginateChannelId <- maybeOrdOriginateChannelId
            ordOriginateChannelType <- OrderChannelType.withValueOpt(ordOriginateChannelId)

            ordTargetChannelId <- maybeOrdTargetChannelId
            ordTargetChannelType <- OrderChannelType.withValueOpt(ordTargetChannelId)
        } yield channelMetric(ordOriginateChannelType, ordTargetChannelType)

        maybeChannelMetric
            .getOrElse(ChannelMetric.Unknown)
            .signalName
    })

    /** Creates tender type metric from a CXI tender type value.
      */
    def extractTenderTypeMetric(tenderType: Int): Option[TenderTypeMetric] = {
        OrderTenderType.withValueOpt(tenderType) match {
            case Some(value) =>
                value match {
                    case OrderTenderType.GiftCard => Some(TenderTypeMetric.GiftCard)
                    case OrderTenderType.CreditCard => Some(TenderTypeMetric.Card)
                    case OrderTenderType.Cash => Some(TenderTypeMetric.Cash)
                    case OrderTenderType.Wallet => Some(TenderTypeMetric.Wallet)
                    case _ => None
                }
            case None =>
                logger.warn(s"Cannot recognize normalized tender type: '$tenderType'")
                None
        }
    }

    val extractTenderTypeMetricUdf = udf((maybeTenderType: Option[Int]) => {
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

package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.normalization

import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import refined_zone.pos_parbrink.model.PaymentStatusType
import support.normalization.udf.{OrderChannelTypeNormalizationUdfs, OrderStateNormalizationUdfs}
import support.normalization.CxiTaxonomyNormalization

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

trait ParbrinkNormalization extends CxiTaxonomyNormalization

case object OrderChannelTypeNormalization extends ParbrinkNormalization {

    private final val NotLetterRegex = "[^a-zA-Z]+"

    val ParbrinkDestinationNameToCxiOriginateChannel = Map(
        "FORHERE" -> OrderChannelType.PhysicalLane,
        "TAKEOUT" -> OrderChannelType.Unknown,
        "PICKUP" -> OrderChannelType.PhysicalLane,
        "DELIVERY" -> OrderChannelType.Unknown,
        "UBEREATS" -> OrderChannelType.DigitalApp,
        "CNUSD" -> OrderChannelType.Unknown,
        "ONLINEPICKUP" -> OrderChannelType.DigitalWeb,
        "POSTMATES" -> OrderChannelType.DigitalApp,
        "GRUBHUB" -> OrderChannelType.DigitalApp,
        "CATERING" -> OrderChannelType.Unknown,
        "OFFICEEXPRESS" -> OrderChannelType.DigitalApp,
        "DOORDASH" -> OrderChannelType.DigitalApp,
        "DRIVETHRU" -> OrderChannelType.PhysicalLane,
        "FOODRUNNERS" -> OrderChannelType.DigitalApp,
        "CURBSIDE" -> OrderChannelType.DigitalWeb,
        "TOGO" -> OrderChannelType.PhysicalLane
    )

    val ParbrinkDestinationNameToCxiTargetChannel = Map(
        "FORHERE" -> OrderChannelType.PhysicalLane,
        "TAKEOUT" -> OrderChannelType.PhysicalPickup,
        "PICKUP" -> OrderChannelType.PhysicalPickup,
        "DELIVERY" -> OrderChannelType.PhysicalDelivery,
        "UBEREATS" -> OrderChannelType.PhysicalDelivery,
        "CNUSD" -> OrderChannelType.Unknown,
        "ONLINEPICKUP" -> OrderChannelType.PhysicalPickup,
        "POSTMATES" -> OrderChannelType.PhysicalDelivery,
        "GRUBHUB" -> OrderChannelType.PhysicalDelivery,
        "CATERING" -> OrderChannelType.PhysicalDelivery,
        "OFFICEEXPRESS" -> OrderChannelType.PhysicalDelivery,
        "DOORDASH" -> OrderChannelType.PhysicalDelivery,
        "DRIVETHRU" -> OrderChannelType.PhysicalPickup,
        "FOODRUNNERS" -> OrderChannelType.PhysicalDelivery,
        "CURBSIDE" -> OrderChannelType.PhysicalPickup,
        "TOGO" -> OrderChannelType.PhysicalPickup
    )

    def normalizeOrderChannelType(
        destinationName: String,
        valueToOrderTenderType: Map[String, OrderChannelType]
    ): OrderChannelType = {

        val maybeValue = Option(destinationName)
            .map(_.replaceAll(NotLetterRegex, ""))
            .map(_.trim)
            .filter(_.nonEmpty)
            .map(_.toUpperCase)

        maybeValue match {
            case Some(value) => valueToOrderTenderType.getOrElse(value, OrderChannelType.Other)
            case None => OrderChannelType.Unknown
        }
    }

    def normalizeOriginateOrderChannelType: UserDefinedFunction =
        normalizeOrderChannelTypeUdf(ParbrinkDestinationNameToCxiOriginateChannel)

    def normalizeTargetOrderChannelType: UserDefinedFunction =
        normalizeOrderChannelTypeUdf(ParbrinkDestinationNameToCxiTargetChannel)

    private def normalizeOrderChannelTypeUdf(
        valueToOrderTenderType: Map[String, OrderChannelType]
    ): UserDefinedFunction =
        OrderChannelTypeNormalizationUdfs.normalizeOrderChannelType((value: String) =>
            normalizeOrderChannelType(value, valueToOrderTenderType)
        )

}

case object PaymentStatusNormalization extends ParbrinkNormalization {

    def normalizePaymentStatus(isDeletedPayment: Option[Boolean], isActiveTender: Option[Boolean]): String = {

        def byActiveTender(value: Option[Boolean]): String = {
            value match {
                case Some(v) => if (v) PaymentStatusType.Active.value else PaymentStatusType.Inactive.value
                case _ => null
            }
        }

        isDeletedPayment match {
            case Some(isDeletedPaymentValue) =>
                if (isDeletedPaymentValue) PaymentStatusType.Deleted.value else byActiveTender(isActiveTender)
            case None => byActiveTender(isActiveTender)
        }
    }

    def normalizePaymentStatus: UserDefinedFunction = {
        udf((isDeletedPayment: Option[Boolean], isActiveTender: Option[Boolean]) =>
            normalizePaymentStatus(isDeletedPayment, isActiveTender)
        )
    }
}

case object OrderStateNormalization extends ParbrinkNormalization {

    def normalizeOrderState: UserDefinedFunction =
        OrderStateNormalizationUdfs.normalizeOrderState((isClosedStr: String) => {
            Try(isClosedStr.toBoolean)
                .map(isClosed =>
                    if (isClosed) {
                        OrderStateType.Completed
                    } else {
                        OrderStateType.Open
                    }
                )
                .getOrElse(OrderStateType.Unknown)
        })

}

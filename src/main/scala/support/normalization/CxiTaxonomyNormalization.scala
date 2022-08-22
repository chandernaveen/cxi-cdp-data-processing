package com.cxi.cdp.data_processing
package support.normalization

import refined_zone.hub.model.{OrderStateType, OrderTenderType}

trait CxiTaxonomyNormalization extends Normalization

case object OrderTenderTypeNormalization extends CxiTaxonomyNormalization {

    def normalizeOrderTenderType(
        tenderType: String,
        valueToOrderTenderType: Map[String, OrderTenderType]
    ): OrderTenderType = {

        val maybeValue = Option(tenderType)
            .map(_.trim)
            .filter(_.nonEmpty)
            .map(_.toUpperCase)

        maybeValue match {
            case Some(value) => valueToOrderTenderType.getOrElse(value, OrderTenderType.Other)
            case None => OrderTenderType.Unknown
        }
    }

}

case object OrderStateNormalization extends CxiTaxonomyNormalization {

    def normalizeOrderState(
        orderState: String,
        valueToOrderStateType: Map[String, OrderStateType]
    ): OrderStateType = {

        val maybeValue = Option(orderState)
            .map(_.trim)
            .filter(_.nonEmpty)
            .map(_.toUpperCase)

        maybeValue match {
            case Some(value) => valueToOrderStateType.getOrElse(value, OrderStateType.Other)
            case None => OrderStateType.Unknown
        }
    }

}

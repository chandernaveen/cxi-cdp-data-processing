package com.cxi.cdp.data_processing
package support.normalization

import refined_zone.hub.model.OrderTenderType
import refined_zone.pos_square.model.PosSquareOrderTenderTypes

sealed trait CxiTaxonomyNormalization extends Normalization

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

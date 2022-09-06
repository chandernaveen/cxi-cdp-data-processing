package com.cxi.cdp.data_processing
package support.normalization.udf

import refined_zone.hub.model.{OrderStateType, OrderTenderType}
import support.normalization.{OrderStateNormalization, OrderTenderTypeNormalization}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.reflect.runtime.universe.TypeTag

trait CxiTaxonomyNormalizationUdfs extends NormalizationUdfs

case object OrderTenderTypeNormalizationUdfs extends CxiTaxonomyNormalizationUdfs {

    def normalizeOrderTenderType(valueToOrderTenderType: Map[String, OrderTenderType]): UserDefinedFunction =
        udf((tenderType: String) =>
            OrderTenderTypeNormalization
                .normalizeOrderTenderType(tenderType, valueToOrderTenderType)
                .code
        )

    def normalizeIntOrderTenderType(valueToOrderTenderType: Map[Int, OrderTenderType]): UserDefinedFunction =
        udf((tenderType: Option[Int]) =>
            tenderType match {
                case Some(value) => valueToOrderTenderType.getOrElse(value, OrderTenderType.Other).code
                case None => OrderTenderType.Unknown.code
            }
        )
}

case object OrderStateNormalizationUdfs extends CxiTaxonomyNormalizationUdfs {

    def normalizeOrderState(valueToOrderStateType: Map[String, OrderStateType]): UserDefinedFunction =
        udf((orderState: String) =>
            OrderStateNormalization
                .normalizeOrderState(orderState, valueToOrderStateType)
                .code
        )

    def normalizeOrderState[A: TypeTag](normalizeFn: A => OrderStateType): UserDefinedFunction = {
        udf((a: A) => normalizeFn(a).code)
    }
}

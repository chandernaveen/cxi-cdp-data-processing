package com.cxi.cdp.data_processing
package support.normalization.udf

import refined_zone.hub.model.OrderTenderType
import support.normalization.OrderTenderTypeNormalization

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

trait CxiTaxonomyNormalizationUdfs extends NormalizationUdfs

case object OrderTenderTypeNormalizationUdfs extends CxiTaxonomyNormalizationUdfs {

    def normalizeOrderTenderType(valueToOrderTenderType: Map[String, OrderTenderType]): UserDefinedFunction =
        udf((tenderType: String) =>
            OrderTenderTypeNormalization
                .normalizeOrderTenderType(tenderType, valueToOrderTenderType)
                .code
        )
}

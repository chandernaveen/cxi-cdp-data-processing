package com.cxi.cdp.data_processing
package support.normalization.udf

import refined_zone.hub.model.OrderChannelType

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.reflect.runtime.universe.TypeTag

case object OrderChannelTypeNormalizationUdfs extends CxiTaxonomyNormalizationUdfs {

    def normalizeOrderChannelType[A: TypeTag](normalizeFn: A => OrderChannelType): UserDefinedFunction = {
        udf((a: A) => normalizeFn(a).code)
    }

    def normalizeOrderChannelType[A1: TypeTag, A2: TypeTag](
        normalizeFn: (A1, A2) => OrderChannelType
    ): UserDefinedFunction = {
        udf((a1: A1, a2: A2) => normalizeFn(a1, a2).code)
    }

    def normalizeOrderChannelType[A1: TypeTag, A2: TypeTag, A3: TypeTag](
        normalizeFn: (A1, A2, A3) => OrderChannelType
    ): UserDefinedFunction = {
        udf((a1: A1, a2: A2, a3: A3) => normalizeFn(a1, a2, a3).code)
    }

    def normalizeOrderChannelType[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
        normalizeFn: (A1, A2, A3, A4) => OrderChannelType
    ): UserDefinedFunction = {
        udf((a1: A1, a2: A2, a3: A3, a4: A4) => normalizeFn(a1, a2, a3, a4).code)
    }

}

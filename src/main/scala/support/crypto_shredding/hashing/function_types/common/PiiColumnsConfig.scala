package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

import support.crypto_shredding.hashing.function_types.common.PiiColumnsConfig.{Column, NormalizationFunc}
import support.crypto_shredding.hashing.transform.TransformFunctions.parseTransformFunction
import support.exceptions.CryptoShreddingException

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityType

case class PiiColumnsConfig(columns: Seq[(Column, NormalizationFunc, Option[IdentityType])]) extends Serializable

object PiiColumnsConfig {

    type Column = String
    type NormalizationFunc = String => String

    def parse(rawConfig: Seq[Map[String, Any]]): PiiColumnsConfig = {
        val columns = rawConfig.map(columnConfig => {
            val column = parseColumn(columnConfig)
            val identityTypeOpt = parseIdentityType(columnConfig)
            val transformFunction = parseTransformFunction(columnConfig)
            (column, transformFunction, identityTypeOpt)
        })
        PiiColumnsConfig(columns)
    }

    private def parseColumn(columnConfig: Map[String, Any]): Column = {
        columnConfig.get("column") match {
            case Some(fieldName: String) => fieldName
            case _ => throw new CryptoShreddingException(s"Unable to parse column config from $columnConfig")
        }
    }

    private def parseIdentityType(columnConfig: Map[String, Any]): Option[IdentityType] = {
        columnConfig.get("identity_type") match {
            case Some(identityTypeCode: String) => Some(IdentityType.withValue(identityTypeCode))
            case _ => Option.empty
        }
    }

}

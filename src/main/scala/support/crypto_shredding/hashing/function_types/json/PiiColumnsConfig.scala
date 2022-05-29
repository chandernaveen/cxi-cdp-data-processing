package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityType
import com.cxi.cdp.data_processing.support.crypto_shredding.hashing.function_types.common.PiiColumnsConfig.Column
import support.crypto_shredding.hashing.function_types.json.PiiColumnsConfig._
import support.crypto_shredding.hashing.transform.TransformFunctions.parseTransformFunction
import support.exceptions.CryptoShreddingException

import scala.util.Try

case class PiiColumnsConfig(columns: Seq[(OuterColumn, InnerColumn, NormalizationFunc, Option[IdentityType])])
    extends Serializable

object PiiColumnsConfig {

    type OuterColumn = String
    type NormalizationFunc = String => String

    sealed trait InnerColumn extends Product with Serializable

    object InnerColumn {

        case class JsonPath(jsonPath: String) extends InnerColumn

    }

    def parse(rawConfig: Seq[Map[String, Any]]): PiiColumnsConfig = {
        val columns = rawConfig.map(columnConfig => {
            val outerColumn = parseOuterColumn(columnConfig)
            val innerColumn = parseInnerColumn(columnConfig)
            val transformFunction = parseTransformFunction(columnConfig)
            val identityTypeOpt = parseIdentityType(columnConfig)
            (outerColumn, innerColumn, transformFunction, identityTypeOpt)
        })
        PiiColumnsConfig(columns)
    }

    private def parseOuterColumn(columnConfig: Map[String, Any]): OuterColumn = {
        columnConfig.get("outerCol") match {
            case Some(fieldName: String) => fieldName
            case _ => throw new CryptoShreddingException(s"Unable to parse outer column config from $columnConfig")
        }
    }

    private def parseInnerColumn(columnConfig: Map[String, Any]): InnerColumn = {
        def parseException = new CryptoShreddingException(s"Unable to parse inner column config from $columnConfig")

        columnConfig.get("innerCol") match {

            case Some(rawInnerColumnConfig: Map[_, _]) =>
                val innerColumnConfig = Try(rawInnerColumnConfig.asInstanceOf[Map[String, String]])
                    .getOrElse(throw parseException)

                innerColumnConfig.get("type") match {
                    case Some("jsonPath") =>
                        val jsonPath = innerColumnConfig.getOrElse("jsonPath", throw parseException)
                        InnerColumn.JsonPath(jsonPath)
                    case _ => throw parseException
                }

            case _ => throw parseException
        }
    }

    private def parseIdentityType(columnConfig: Map[String, Any]): Option[IdentityType] = {
        columnConfig.get("identity_type") match {
            case Some(identityTypeCode: String) => Some(IdentityType.withValue(identityTypeCode))
            case _ => Option.empty
        }
    }

}

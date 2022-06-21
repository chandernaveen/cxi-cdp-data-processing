package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.transform

import support.exceptions.CryptoShreddingException
import support.normalization.AdvertiserIdNormalization.normalizeAdvertiserId
import support.normalization.EmailNormalization.normalizeEmail
import support.normalization.PhoneNumberNormalization.normalizePhoneNumber

object TransformFunctions {

    final val NormalizeEmailTransformationName = "normalizeEmail"
    final val NormalizePhoneNumberTransformationName = "normalizePhoneNumber"
    final val NormalizeAdvertiserIdTransformationName = "normalizeAdvertiserId"

    def get(key: String): Option[String => String] = {
        transformFunctions.get(key)
    }

    def parseTransformFunction(config: Map[String, Any]): String => String = {
        val parsingException = new CryptoShreddingException(s"Unable to parse transformation config from $config")
        config.get("transform") match {
            case Some(transformConfig: Map[String, String]) =>
                transformConfig.get("transformationName") match {
                    case Some(transformationName) =>
                        TransformFunctions.get(transformationName) match {
                            case Some(value) => value
                            case _ => throw parsingException
                        }
                    case _ => throw parsingException
                }
            case _ => identity
        }
    }

    private val transformFunctions: Map[String, String => String] = Map(
        NormalizeEmailTransformationName -> transformEmail,
        NormalizePhoneNumberTransformationName -> transformPhoneNumber,
        NormalizeAdvertiserIdTransformationName -> transformAdvertiserId
    )

    private def transformAdvertiserId(value: String): String = {
        normalizeAdvertiserId(value).orNull
    }

    private def transformPhoneNumber(value: String): String = {
        normalizePhoneNumber(value).orNull
    }

    private def transformEmail(value: String): String = {
        normalizeEmail(value).orNull
    }

}

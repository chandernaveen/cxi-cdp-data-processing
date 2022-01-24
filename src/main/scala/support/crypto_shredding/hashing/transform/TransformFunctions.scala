package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.transform

import support.exceptions.CryptoShreddingException

object TransformFunctions {

    private final val notNumbers = "[^0-9]".r

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
        "normalizeEmail" -> normalizeEmail,
        "normalizePhoneNumber" -> normalizePhoneNumber
    )

    def normalizeEmail(value: String): String =  {
        value.toLowerCase()
    }

    def normalizePhoneNumber(value: String): String =  {
        var transformed = notNumbers.replaceAllIn(value, "")
        if (transformed.length < 11) {
            transformed = "1" + transformed
        }
        transformed
    }

}

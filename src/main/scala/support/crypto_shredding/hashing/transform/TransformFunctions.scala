package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.transform

import support.exceptions.CryptoShreddingException

import com.google.i18n.phonenumbers.{NumberParseException, PhoneNumberUtil}
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat
import org.apache.commons.validator.routines.EmailValidator

object TransformFunctions {

    final val NormalizeEmailTransformationName = "normalizeEmail"
    final val NormalizePhoneNumberTransformationName = "normalizePhoneNumber"

    private final val EmailValidatorInstance = EmailValidator.getInstance()
    private final val PhoneNumberUtilInstance = PhoneNumberUtil.getInstance()

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
        NormalizeEmailTransformationName -> normalizeEmail,
        NormalizePhoneNumberTransformationName -> normalizePhoneNumber
    )

    def normalizeEmail(value: String): String = {
        Some(value)
            .filter(_ != null)
            .map(_.trim)
            .filter(_.nonEmpty)
            .filter(email => EmailValidatorInstance.isValid(email))
            .map(_.toLowerCase)
            .orNull
    }

    def normalizePhoneNumber(value: String): String = {
        try {
            Some(value)
                .filter(_ != null)
                .map(_.trim)
                .filter(_.nonEmpty)
                .map(value => PhoneNumberUtilInstance.parse(value, "US"))
                .filter(phoneNumber => PhoneNumberUtilInstance.isValidNumber(phoneNumber))
                .map(phoneNumber => PhoneNumberUtilInstance.format(phoneNumber, PhoneNumberFormat.E164))
                // remove leading '+'
                .map(_.substring(1))
                .orNull
        } catch {
            case _: NumberParseException => null
        }
    }

}

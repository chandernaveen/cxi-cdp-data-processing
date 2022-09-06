package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Type, Weight}
import refined_zone.pos_parbrink.processor.identity.PosIdentityProcessor.HashFunctionConfig
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

object PaymentsIdentitiesProcessor {

    val LastFirstNameDelimiter = "/"
    val HolderNameBrandPanDelimiter = "-"

    def computeIdentitiesFromPayments(payments: DataFrame, cryptoShredding: CryptoShredding): DataFrame = {
        val identities = payments
            .withColumn(CxiIdentityId, createPaymentIdentity(col("name"), col("card_brand"), col("pan")))
            .filter(col(CxiIdentityId).isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                col(CxiIdentityId),
                lit(IdentityType.CardHolderNameTypeNumber.code).as(Type),
                lit(2).as(Weight)
            )

        val hashedIdentities =
            cryptoShredding.applyHashCryptoShredding("common", HashFunctionConfig, identities)

        hashedIdentities
    }

    def createCardHolderNameTypeNumberIdentity(holderName: String, cardBrand: String, pan: String): Option[String] = {

        def isEmpty(s: String) = s == null || s.trim.isEmpty

        if (isEmpty(holderName) || isEmpty(cardBrand) || isEmpty(pan)) {
            None
        } else {
            val lastFirstName = holderName.split(LastFirstNameDelimiter)
            if (lastFirstName.size < 2) {
                None
            } else {
                val lastName = lastFirstName(0)
                val firstName = lastFirstName(1)

                if (isEmpty(lastName) || isEmpty(firstName)) {
                    None
                } else {
                    Some(
                        Seq(s"${lastName.trim}${LastFirstNameDelimiter}${firstName.trim}", cardBrand.trim, pan.trim)
                            .mkString(HolderNameBrandPanDelimiter)
                            .toLowerCase
                    )
                }
            }
        }
    }

    def createPaymentIdentity: UserDefinedFunction = {
        udf((name: String, cardBrand: String, pan: String) =>
            createCardHolderNameTypeNumberIdentity(name, cardBrand, pan)
        )
    }

}

package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types

case class CryptoHashingResult(original_value: String, hashed_value: String, identity_type: String) extends Serializable

object CryptoHashingResult {
    val OriginalValueColName = "original_value"
    val HashedValueColName = "hashed_value"
    val IdentityTypeValueColName = "identity_type"
}

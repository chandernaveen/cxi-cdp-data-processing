package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

case class CommonOutputValue(isSucceeded: Boolean, pii: String, hash: String) extends Serializable

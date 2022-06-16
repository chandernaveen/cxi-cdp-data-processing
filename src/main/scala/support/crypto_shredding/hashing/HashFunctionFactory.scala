package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing

import support.crypto_shredding.hashing.function_types.common.CommonHashingFunction
import support.crypto_shredding.hashing.function_types.json.JsonHashingFunction
import support.crypto_shredding.hashing.function_types.IHashFunction

/** The HashFunctionFactory will allow us to call multiple different classes using the same Object
  */
object HashFunctionFactory {

    def getFunction(hashFunctionType: String, hashFunctionConfig: Map[String, Any], salt: String): IHashFunction = {
        hashFunctionType match {
            case "common" => new CommonHashingFunction(hashFunctionConfig, salt)
            case "json" => new JsonHashingFunction(hashFunctionConfig, salt)
            case _ => throw new IllegalArgumentException(s"Unknown type '${hashFunctionType}'.")
        }
    }
}

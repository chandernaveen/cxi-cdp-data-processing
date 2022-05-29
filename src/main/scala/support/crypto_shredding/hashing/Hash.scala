package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing

import java.math.BigInteger
import java.security.MessageDigest

object Hash {
    def sha256Hash(id: String, salt: String = ""): String = {
        val messageDigest = MessageDigest.getInstance("SHA-256")
        val bytes = messageDigest.digest((id + salt).getBytes("UTF-8"))
        String.format("%064x", new BigInteger(1, bytes))
    }
}

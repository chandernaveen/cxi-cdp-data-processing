package com.cxi.cdp.data_processing
package support.exceptions

class CryptoShreddingException private (ex: RuntimeException) extends RuntimeException(ex) {
    def this(message: String) = this(new RuntimeException(message))
    def this(message: String, throwable: Throwable) = this(new RuntimeException(message, throwable))
}

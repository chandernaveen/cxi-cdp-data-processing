package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.transform

import support.exceptions.CryptoShreddingException

import org.scalatest.{FunSuite, Matchers}

class TransformFunctionsTest extends FunSuite with Matchers {

    test("Parse transform config - exception if 'transformationName' key is absent") {
        // given
        val config = Map("transform" -> Map("not_transformation_name_key" -> "something"))
        // when
        val exception = the[CryptoShreddingException] thrownBy TransformFunctions.parseTransformFunction(config)
        // then
        exception.getMessage shouldBe s"java.lang.RuntimeException: Unable to parse transformation config from $config"
    }

    test("Parse transform config - exception if transformation function is absent") {
        // given
        val config = Map("transform" -> Map("transformationName" -> "non-existent-function"))
        // when
        val exception = the[CryptoShreddingException] thrownBy TransformFunctions.parseTransformFunction(config)
        // then
        exception.getMessage shouldBe s"java.lang.RuntimeException: Unable to parse transformation config from $config"
    }

    test("Parse transform config - identity function if no transformation config") {
        // given
        val config = Map("not_transform" -> "something")
        // when
        val function = TransformFunctions.parseTransformFunction(config)
        // then
        function("SomeStrinG") shouldBe "SomeStrinG"
    }

    test("Parse transform config - normalizeEmail function") {
        // given
        val config = Map("transform" -> Map("transformationName" -> "normalizeEmail"))
        // when
        val function = TransformFunctions.parseTransformFunction(config)
        // then
        function("PAUL123@mailbox.com") shouldBe "paul123@mailbox.com"
    }

    test("Parse transform config - normalizePhoneNumber function") {
        // given
        val config = Map("transform" -> Map("transformationName" -> "normalizePhoneNumber"))
        // when
        val function = TransformFunctions.parseTransformFunction(config)
        // then
        function("+1 212.456.7890") shouldBe "12124567890"
    }

    test("Parse transform config - normalizeAdvertiserId function") {
        // given
        val config = Map("transform" -> Map("transformationName" -> "normalizeAdvertiserId"))
        // when
        val function = TransformFunctions.parseTransformFunction(config)
        // then
        function("Abcd1234-Ef56-Gh78-iJ90-klmO1234pqrS") shouldBe "ABCD1234-EF56-GH78-IJ90-KLMO1234PQRS"
    }

}

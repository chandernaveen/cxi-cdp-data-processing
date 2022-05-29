package com.cxi.cdp.data_processing
package refined_zone.service

import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

class MetadataServiceTest extends FunSuite {

    case class TestCase(identityType: String, originalValue: String, expected: Map[String, String])

    test("Extract metadata from different types of identities") {
        // given
        val testCases = Seq(
            TestCase("phone", "12345678912", Map("phone_area_code" -> "1234")),
            TestCase("email", "fname@mailbox.com", Map("email_domain" -> "mailbox.com")),
            TestCase("unknown", "1234something@something", Map.empty[String, String]),
            TestCase("phone", "123", Map.empty[String, String]),
            TestCase("email", "123", Map.empty[String, String]),
            TestCase("email", "123@", Map.empty[String, String]),
            TestCase("email", null, Map.empty[String, String]),
            TestCase("phone", null, Map.empty[String, String])
        )

        // when
        for (TestCase(identityType, originalValue, expected) <- testCases) {
            MetadataService.extractMetadata(identityType, originalValue) shouldBe expected
        }
    }

}

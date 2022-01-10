package com.cxi.cdp.data_processing
package support.cleansing

import org.scalatest.{FunSuite, Matchers}

class LocationCleansingTest extends FunSuite with Matchers {

    import LocationCleansing._

    test("cleanseZipCode for a 5-digit ZIP Code") {
        cleanseZipCode("12345") shouldBe Some("12345")
    }

    test("cleanseZipCode for a ZIP+4 Code") {
        cleanseZipCode("98765-4321") shouldBe Some("98765")
    }

    test("cleanseZipCode for incorrect ZIP Code") {
        cleanseZipCode("ZIP Code: 98765-4321") shouldBe None
        cleanseZipCode("1234") shouldBe None
        cleanseZipCode(null) shouldBe None
    }

}

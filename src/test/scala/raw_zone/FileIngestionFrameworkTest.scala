package com.cxi.cdp.data_processing
package raw_zone

import java.time.LocalDate

import org.scalatest.{FunSuite, Matchers}

class FileIngestionFrameworkTest extends FunSuite with Matchers {

    import FileIngestionFramework.CliArgs

    test("CliArgs parses valid args") {
        val args = Seq("/path/to/contract.json", "2021-10-15")
        CliArgs.parse(args) shouldBe CliArgs("/path/to/contract.json", LocalDate.of(2021, 10, 15))
    }

    test("CliArgs parses incorrect number of args") {
        val args = Seq("first", "second", "third")
        assertThrows[IllegalArgumentException](CliArgs.parse(args))
    }

    test("CliArgs parses invalid date") {
        val args = Seq("/path/to/contract.json", "October 15, 2021")
        assertThrows[IllegalArgumentException](CliArgs.parse(args))
    }

}

package com.cxi.cdp.data_processing
package raw_zone

import org.scalatest.{FunSuite, Matchers}

import java.time.LocalDate

class FileIngestionFrameworkCliArgsTest extends FunSuite with Matchers {

    import FileIngestionFramework.CliArgs

    test("CliArgs parses valid args") {
        val args = Seq("/path/to/contract.json", "2021-10-15", "yyyy-MM-dd")
        CliArgs.parse(args) shouldBe CliArgs("/path/to/contract.json", LocalDate.of(2021, 10, 15), "yyyy-MM-dd")
    }

    test("CliArgs parses incorrect number of args") {
        val args = Seq("first", "2022-07-07", "runId", "yyyyMMdd", "extra argument")
        assertThrows[IllegalArgumentException](CliArgs.parse(args))
    }

    test("CliArgs parses invalid date") {
        val args = Seq("/path/to/contract.json", "October 15, 2021", "runId")
        assertThrows[IllegalArgumentException](CliArgs.parse(args))
    }

}

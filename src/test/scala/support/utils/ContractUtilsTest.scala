package com.cxi.cdp.data_processing
package support.utils

import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

class ContractUtilsTest extends FunSuite {

    test("test propIsSet") {
        // given
        val jsonContent =
            s"""
               |{
               |  "db_name": "orders"
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val isDbNamePropSet = contract.propIsSet("db_name")
        val isTableNamePropSet = contract.propIsSet("table_name")

        // then
        isDbNamePropSet shouldBe true
        isTableNamePropSet shouldBe false
    }

    test("test prop with Int type") {
        // given
        val jsonContent =
            s"""
               |{
               |  "max_threshold": 1
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val maxThreshold = contract.prop[Int]("max_threshold")

        // then
        maxThreshold shouldBe 1
    }

    test("test prop with String type") {
        // given
        val jsonContent =
            s"""
               |{
               |  "transformationName1": "identity"
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val transformationName1 = contract.prop[String]("transformationName1")

        // then
        transformationName1 shouldBe "identity"
    }

    test("test getProperty") {
        // given
        val jsonContent =
            s"""
               |{
               |  "max_threshold": 1
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val maxThreshold = contract.getProperty("max_threshold")
        val minThreshold = contract.getProperty("min_threshold")

        // then
        maxThreshold shouldBe Some(1)
        minThreshold shouldBe None
    }

    test("test propToString") {
        // given
        val jsonContent =
            s"""
               |{
               |  "max_threshold": 1,
               |  "db_name": "locations",
               |  "json_array_prop": [true, false]
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val maxThreshold = contract.propToString("max_threshold")
        val dbName = contract.propToString("db_name")
        val jsonArrayProp = contract.propToString("json_array_prop")

        // then
        maxThreshold shouldBe "max_threshold : 1"
        dbName shouldBe """db_name : "locations""""
        jsonArrayProp shouldBe """json_array_prop : [ true, false ]"""
    }

    test("test propOrElse with Int type") {
        // given
        val jsonContent =
            s"""
               |{
               |  "max_threshold": 1
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val maxThreshold = contract.propOrElse[Int]("max_threshold", 15)
        val minThreshold = contract.propOrElse[Int]("min_threshold", 25)

        // then
        maxThreshold shouldBe 1
        minThreshold shouldBe 25
    }

    test("test prop with List[String] type") {
        // given
        val jsonContent =
            s"""
               |{
               |  "config": {
               |    "init_scripts": ["init_script1", "init_script2"]
               |  }
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val initScripts = contract.prop[List[String]]("config.init_scripts")

        // then
        initScripts shouldBe List("init_script1", "init_script2")
    }

    test("test prop with Map[String, Int] type") {
        // given
        val jsonContent =
            s"""
               |{
               |  "config": {
               |    "props": {
               |        "key1": 1,
               |        "key2": 2
               |    }
               |  }
               |}
               |""".stripMargin
        val contract = new ContractUtils(jsonContent)

        // when
        val props = contract.prop[Map[String, Int]]("config.props")

        // then
        props shouldBe Map("key1" -> 1, "key2" -> 2)
    }

    test("test prop - non existent prop") {
        // given
        val contract = new ContractUtils("{}")

        // when
        assertThrows[IllegalArgumentException] {
            val someProp = contract.prop[String]("some_prop")
        }
    }

}

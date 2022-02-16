package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import support.crypto_shredding.hashing.transform.TransformFunctions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.scalatest.{FunSuite, Matchers}

class JsonNodeHasherTest extends FunSuite with Matchers {

    import PiiColumnsConfig._

    val piiConfig: PiiColumnsConfig = PiiColumnsConfig(Seq(
        ("record_value", InnerColumn.JsonPath("$.pii_column"), identity, Option.empty),
        ("record_value", InnerColumn.JsonPath("$.nested.column"), identity, Option.empty),
        ("record_value", InnerColumn.JsonPath("$.nested..recursive_column"), identity, Option.empty)
    ))

    val piiConfigWithTransform: PiiColumnsConfig = PiiColumnsConfig(Seq(
        ("record_value", InnerColumn.JsonPath("$.pii_column"), TransformFunctions.get("normalizeEmail").get, Option.empty),
        ("record_value", InnerColumn.JsonPath("$.nested.column"), TransformFunctions.get("normalizeEmail").get, Option.empty),
        ("record_value", InnerColumn.JsonPath("$.nested..recursive_column"), TransformFunctions.get("normalizeEmail").get, Option.empty)
    ))

    val mapper = new ObjectMapper with ScalaObjectMapper
    val salt = "NaCl"
    val hasher = new JsonNodeHasher(piiConfig, salt, mapper)
    val hasherWithTransformFunction = new JsonNodeHasher(piiConfigWithTransform, salt, mapper)

    test("test json-path hash with non-matching columns") {
        // given
        val input = mapper.readTree(
            s"""
                {
                  "another_record_value": {
                     "pii_column": "pii_column_value_will_not_change"
                   }
                }
            """)

        // when
        val actual = hasher.apply(input)

        // then
        val expected = mapper.readTree(
            s"""
                {
                  "another_record_value" : {
                    "pii_column" : "pii_column_value_will_not_change"
                  },
                  "hashed_data":[]
                }
            """)

        actual shouldBe expected
    }

    test("test json-path hash with matching columns") {
        // given
        val input = mapper.readTree(
            s"""
                {
                  "record_value": {
                    "regular_column": "regular_column_value",
                    "pii_column": "pii_column_value",
                    "nested": {
                      "column": "nested_column_value",
                      "nested_regular_column": "nested_regular_column_value",
                      "recursive_column": "nested_recursive_value",
                      "nested_objects": [
                        { "recursive_column": "first_recursive_column_value" },
                        {
                          "second": {
                            "recursive_column": "second_recursive_column_value",
                            "another_regular_column": "another_regular_column_value"
                          }
                        },
                        { "third": { "recursive_column": null } }
                      ]
                    }
                  },
                  "another_record_value": {
                     "pii_column": "pii_column_value_will_not_change"
                   }
                }
            """)

        // when
        val actual = hasher.apply(input)

        // then
        val expected = mapper.readTree(
            s"""
                {
                  "record_value" : {
                    "regular_column" : "regular_column_value",
                    "pii_column" : "3130bd4ad0a7561677969ee92e9773daddbdad31a002f97f326d9bff2128c28d",
                    "nested" : {
                      "column" : "f4ea2420d0952ddd3f2d8f73592ba7e71042058ad3a7db6d1ed1949c68cb3e4b",
                      "nested_regular_column" : "nested_regular_column_value",
                      "recursive_column" : "41d814bb4143d8781ddf03b5a18f08b93141a2b1b0a48fc4b98957751e78e040",
                      "nested_objects" : [
                        { "recursive_column" : "8fdf4987616c3f75c31394b7d7ea355c2804e031e61af946a89922b5ceba07ee"},
                        {
                          "second" : {
                            "recursive_column" : "a635dc72df1525bb09c80b3945c0e876b2d212a3e471577b212b879253bb2210",
                            "another_regular_column" : "another_regular_column_value"
                          }
                        },
                        { "third": { "recursive_column": null } }
                      ]
                    }
                  },
                  "another_record_value" : {
                    "pii_column" : "pii_column_value_will_not_change"
                  },
                  "hashed_data" : [ {
                    "original_value" : "pii_column_value",
                    "hashed_value" : "3130bd4ad0a7561677969ee92e9773daddbdad31a002f97f326d9bff2128c28d",
                    "identity_type" : null
                  }, {
                    "original_value" : "nested_column_value",
                    "hashed_value" : "f4ea2420d0952ddd3f2d8f73592ba7e71042058ad3a7db6d1ed1949c68cb3e4b",
                    "identity_type" : null
                  }, {
                    "original_value" : "nested_recursive_value",
                    "hashed_value" : "41d814bb4143d8781ddf03b5a18f08b93141a2b1b0a48fc4b98957751e78e040",
                    "identity_type" : null
                  }, {
                    "original_value" : "first_recursive_column_value",
                    "hashed_value" : "8fdf4987616c3f75c31394b7d7ea355c2804e031e61af946a89922b5ceba07ee",
                    "identity_type" : null
                  }, {
                    "original_value" : "second_recursive_column_value",
                    "hashed_value" : "a635dc72df1525bb09c80b3945c0e876b2d212a3e471577b212b879253bb2210",
                    "identity_type" : null
                  } ]
                }
            """)

        actual shouldBe expected
    }

    test("test json-path hash with non-matching columns (with transform functions)") {
        // given
        val input = mapper.readTree(
            s"""
                {
                  "another_record_value": {
                     "pii_column": "pii_column_value_will_not_change"
                   }
                }
            """)

        // when
        val actual = hasherWithTransformFunction.apply(input)

        // then
        val expected = mapper.readTree(
            s"""
                {
                  "another_record_value" : {
                    "pii_column" : "pii_column_value_will_not_change"
                  },
                  "hashed_data":[]
                }
            """)

        actual shouldBe expected
    }

    test("test json-path hash with matching columns (with transform functions)") {
        // given
        val input = mapper.readTree(
            s"""
                {
                  "record_value": {
                    "regular_column": "regular_column_value",
                    "pii_column": "PII_column_VALUE",
                    "nested": {
                      "column": "Nested_column_VALUE",
                      "nested_regular_column": "nested_regular_column_value",
                      "recursive_column": "Nested_Recursive_ValuE",
                      "nested_objects": [
                        { "recursive_column": "First_Recursive_Column_Value" },
                        {
                          "second": {
                            "recursive_column": "Second_Recursive_Column_Value",
                            "another_regular_column": "another_regular_column_value"
                          }
                        },
                        { "third": { "recursive_column": null } }
                      ]
                    }
                  },
                  "another_record_value": {
                     "pii_column": "pii_column_value_will_not_change"
                   }
                }
            """)

        // when
        val actual = hasherWithTransformFunction.apply(input)

        // then
        val expected = mapper.readTree(
            s"""
                {
                  "record_value" : {
                    "regular_column" : "regular_column_value",
                    "pii_column" : "3130bd4ad0a7561677969ee92e9773daddbdad31a002f97f326d9bff2128c28d",
                    "nested" : {
                      "column" : "f4ea2420d0952ddd3f2d8f73592ba7e71042058ad3a7db6d1ed1949c68cb3e4b",
                      "nested_regular_column" : "nested_regular_column_value",
                      "recursive_column" : "41d814bb4143d8781ddf03b5a18f08b93141a2b1b0a48fc4b98957751e78e040",
                      "nested_objects" : [
                        { "recursive_column" : "8fdf4987616c3f75c31394b7d7ea355c2804e031e61af946a89922b5ceba07ee"},
                        {
                          "second" : {
                            "recursive_column" : "a635dc72df1525bb09c80b3945c0e876b2d212a3e471577b212b879253bb2210",
                            "another_regular_column" : "another_regular_column_value"
                          }
                        },
                        { "third": { "recursive_column": null } }
                      ]
                    }
                  },
                  "another_record_value" : {
                    "pii_column" : "pii_column_value_will_not_change"
                  },
                  "hashed_data" : [ {
                    "original_value" : "pii_column_value",
                    "hashed_value" : "3130bd4ad0a7561677969ee92e9773daddbdad31a002f97f326d9bff2128c28d",
                    "identity_type" : null
                  }, {
                    "original_value" : "nested_column_value",
                    "hashed_value" : "f4ea2420d0952ddd3f2d8f73592ba7e71042058ad3a7db6d1ed1949c68cb3e4b",
                    "identity_type" : null
                  }, {
                    "original_value" : "nested_recursive_value",
                    "hashed_value" : "41d814bb4143d8781ddf03b5a18f08b93141a2b1b0a48fc4b98957751e78e040",
                    "identity_type" : null
                  }, {
                    "original_value" : "first_recursive_column_value",
                    "hashed_value" : "8fdf4987616c3f75c31394b7d7ea355c2804e031e61af946a89922b5ceba07ee",
                    "identity_type" : null
                  }, {
                    "original_value" : "second_recursive_column_value",
                    "hashed_value" : "a635dc72df1525bb09c80b3945c0e876b2d212a3e471577b212b879253bb2210",
                    "identity_type" : null
                  } ]
                }
            """)

        actual shouldBe expected
    }
}

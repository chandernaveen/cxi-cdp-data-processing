package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import support.crypto_shredding.hashing.transform.TransformFunctions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.scalatest.{FunSuite, Matchers}

class JsonNodeHasherTest extends FunSuite with Matchers {

    import PiiColumnsConfig._

    val piiConfig: PiiColumnsConfig = PiiColumnsConfig(
        Seq(
            ("record_value", InnerColumn.JsonPath("$.pii_column"), identity, Option.empty),
            ("record_value", InnerColumn.JsonPath("$.nested.column"), identity, Option.empty),
            ("record_value", InnerColumn.JsonPath("$.nested..recursive_column"), identity, Option.empty)
        )
    )

    val piiConfigWithTransform: PiiColumnsConfig = PiiColumnsConfig(
        Seq(
            (
                "record_value",
                InnerColumn.JsonPath("$.pii_column"),
                TransformFunctions.get("normalizeEmail").get,
                Option.empty
            ),
            (
                "record_value",
                InnerColumn.JsonPath("$.nested.column"),
                TransformFunctions.get("normalizeEmail").get,
                Option.empty
            ),
            (
                "record_value",
                InnerColumn.JsonPath("$.nested..recursive_column"),
                TransformFunctions.get("normalizeEmail").get,
                Option.empty
            )
        )
    )

    val mapper = new ObjectMapper with ScalaObjectMapper
    val salt = "NaCl"
    val hasher = new JsonNodeHasher(piiConfig, salt, mapper)
    val hasherWithTransformFunction = new JsonNodeHasher(piiConfigWithTransform, salt, mapper)

    test("test json-path hash with non-matching columns") {
        // given
        val input = mapper.readTree(s"""
                {
                  "another_record_value": {
                     "pii_column": "pii_column_value_will_not_change"
                   }
                }
            """)

        // when
        val actual = hasher.apply(input)

        // then
        val expected = mapper.readTree(s"""
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
        val input = mapper.readTree(s"""
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
        val expected = mapper.readTree(s"""
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
        val input = mapper.readTree(s"""
                {
                  "another_record_value": {
                     "pii_column": "pii_column_value_will_not_change"
                   }
                }
            """)

        // when
        val actual = hasherWithTransformFunction.apply(input)

        // then
        val expected = mapper.readTree(s"""
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
        val input = mapper.readTree(s"""
                {
                  "record_value": {
                    "regular_column": "regular_column_value",
                    "pii_column": "PAUL123@mailbox.com",
                    "nested": {
                      "column": "GEORGE@mailbox.COM",
                      "nested_regular_column": "nested_regular_column_value",
                      "recursive_column": "rinGO@mailboX.com",
                      "nested_objects": [
                        { "recursive_column": "JoHn@MaIlbox.com" },
                        {
                          "second": {
                            "recursive_column": "WhoAmI@MaIlbox.com",
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
        val expected = mapper.readTree(s"""
                {
                  "record_value" : {
                    "regular_column" : "regular_column_value",
                    "pii_column" : "d0017559375f5bf341c336b378f0a5a269cb870b6046abe3dde9344c3bfdee6b",
                    "nested" : {
                      "column" : "2a98a0c496851f2511cd64f7fbc80eb51c6fa2addfb4bb8406b4a446ba85b3ec",
                      "nested_regular_column" : "nested_regular_column_value",
                      "recursive_column" : "bf6ede98454256ef8c0fce05ed7053ea594c5cc045b10102db16e6e9963223fc",
                      "nested_objects" : [
                        { "recursive_column" : "cf607a184b327044d23085389f4692f4c1ab23c97a7c2d79f089dfe1c0df2a95"},
                        {
                          "second" : {
                            "recursive_column" : "03343b1b9a59c41e26aa9027094ed7ddfbc1fc5ef4afcdae9ce8cda0bd1c3726",
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
                    "original_value" : "paul123@mailbox.com",
                    "hashed_value" : "d0017559375f5bf341c336b378f0a5a269cb870b6046abe3dde9344c3bfdee6b",
                    "identity_type" : null
                  }, {
                    "original_value" : "george@mailbox.com",
                    "hashed_value" : "2a98a0c496851f2511cd64f7fbc80eb51c6fa2addfb4bb8406b4a446ba85b3ec",
                    "identity_type" : null
                  }, {
                    "original_value" : "ringo@mailbox.com",
                    "hashed_value" : "bf6ede98454256ef8c0fce05ed7053ea594c5cc045b10102db16e6e9963223fc",
                    "identity_type" : null
                  }, {
                    "original_value" : "john@mailbox.com",
                    "hashed_value" : "cf607a184b327044d23085389f4692f4c1ab23c97a7c2d79f089dfe1c0df2a95",
                    "identity_type" : null
                  }, {
                    "original_value" : "whoami@mailbox.com",
                    "hashed_value" : "03343b1b9a59c41e26aa9027094ed7ddfbc1fc5ef4afcdae9ce8cda0bd1c3726",
                    "identity_type" : null
                  } ]
                }
            """)

        actual shouldBe expected
    }
}

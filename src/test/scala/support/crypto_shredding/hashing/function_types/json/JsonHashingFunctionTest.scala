package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class JsonHashingFunctionTest extends BaseSparkBatchJobTest {

    test("test json hash for one pii field which is one level deep") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map(
            "pii_columns" -> List(
                Map("outerCol" -> "record_value",
                    "innerCol" -> Map("type" -> "jsonPath", "jsonPath" -> "$.pii_column"))))
        val jsonHashingFunction = new JsonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            (s"""{"pii_column": "Bob", "some_other_col_inside_json_object" : 1}""", 10),
            (s"""{"pii_column": "Alice", "some_other_col_inside_json_object" : 2}""", 20),
            (s"""{"pii_column": "John", "some_other_col_inside_json_object" : 3}""", 30)
        ).toDF("record_value", "some_other_top_level_column")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = jsonHashingFunction.hash(landingData)

        // then
        // check pii df
        val actualFieldsReturnedForPiiDf = extractedPersonalInformationDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for pii data frame:\n" + extractedPersonalInformationDf.schema.treeString) {
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                ("Bob", "cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961"),
                ("Alice", "3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043"),
                ("John", "a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da")
            ).toDF("original_value", "hashed_value").collect()
            actualExtractedPiiData.length should equal(expected.length)
            actualExtractedPiiData should contain theSameElementsAs expected
        }
        // check hashed df
        val actualFieldsReturnedForHashedDf = hashedOriginalDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for hashed data frame:\n" + hashedOriginalDf.schema.treeString) {
            actualFieldsReturnedForHashedDf shouldEqual Array("record_value", "some_other_top_level_column")
        }
        val actualHashedOriginalDfData = hashedOriginalDf.collect()
        withClue("Actual hashed data frame data do not match") {
            val expected = List(
                (s"""{"pii_column":"cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961","some_other_col_inside_json_object":1}""", 10),
                (s"""{"pii_column":"3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043","some_other_col_inside_json_object":2}""", 20),
                (s"""{"pii_column":"a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da","some_other_col_inside_json_object":3}""", 30)
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }
    }

    test("test json hash with pii data absent for some of the records") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map(
            "pii_columns" -> List(
                Map("outerCol" -> "record_value",
                    "innerCol" -> Map("type" -> "jsonPath", "jsonPath" -> "$.pii_credit_card_column"))))
        val jsonHashingFunction = new JsonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            (s"""{"some_other_col_inside_json_object" : 9}""", 90),
            (s"""{"pii_credit_card_column": "****5918", "some_other_col_inside_json_object" : 8}""", 80),
            (s"""{"pii_credit_card_column": "****1892"}""", 70),
        ).toDF("record_value", "some_other_top_level_column")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = jsonHashingFunction.hash(landingData)

        // then
        // check pii df
        val actualFieldsReturnedForPiiDf = extractedPersonalInformationDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for pii data frame:\n" + extractedPersonalInformationDf.schema.treeString) {
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                ("****5918", "19d8e1a307d593d525c4f9b5372b8fc38ea8f9abe31a89359d013c18e21b1c8e"),
                ("****1892", "f343a79ac41d584201b7a5bc9536c503b876de1bed2b528602c1cd55b141c660"),
            ).toDF("original_value", "hashed_value").collect()
            actualExtractedPiiData.length should equal(expected.length)
            actualExtractedPiiData should contain theSameElementsAs expected
        }
        // check hashed df
        val actualFieldsReturnedForHashedDf = hashedOriginalDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for hashed data frame:\n" + hashedOriginalDf.schema.treeString) {
            actualFieldsReturnedForHashedDf shouldEqual Array("record_value", "some_other_top_level_column")
        }
        val actualHashedOriginalDfData = hashedOriginalDf.collect()
        withClue("Actual hashed data frame data do not match") {
            val expected = List(
                (s"""{"some_other_col_inside_json_object":9}""", 90),
                (s"""{"pii_credit_card_column":"19d8e1a307d593d525c4f9b5372b8fc38ea8f9abe31a89359d013c18e21b1c8e","some_other_col_inside_json_object":8}""", 80),
                (s"""{"pii_credit_card_column":"f343a79ac41d584201b7a5bc9536c503b876de1bed2b528602c1cd55b141c660"}""", 70)
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }
    }

    test("test json hash with multiple pii columns") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map(
            "pii_columns" -> List(
                Map("outerCol" -> "record_value",
                    "innerCol" -> Map("type" -> "jsonPath", "jsonPath" -> "$.pii_credit_card_column")),
                Map("outerCol" -> "record_value",
                    "innerCol" -> Map("type" -> "jsonPath", "jsonPath" -> "$.pii_customer_name"))))
        val jsonHashingFunction = new JsonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            (s"""{"pii_credit_card_column": "****1234", "pii_customer_name" : "Paul", "some_other_col_inside_json_object" : 9}""", 40),
            (s"""{"pii_credit_card_column": "****5918", "pii_customer_name" : "George"}""", 50),
            (s"""{"pii_credit_card_column": "****1892", "pii_customer_name" : "Ringo"}""", 60),
        ).toDF("record_value", "some_other_top_level_column")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = jsonHashingFunction.hash(landingData)

        // then
        // check pii df
        val actualFieldsReturnedForPiiDf = extractedPersonalInformationDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for pii data frame:\n" + extractedPersonalInformationDf.schema.treeString) {
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                ("Paul", "818b5cc5f21d3e6e4e6071c06294528d44595022218446d8b79304d2b766327a"),
                ("George", "3d28271ec52e3d07fe14f5f16d01f2c09cbcac1949f9904b305136d0edbee12d"),
                ("Ringo", "1ec3fb2651897b571d646d06b431e5deb7de3a9ac2b283ffa124c5a20805f501"),
                ("****1234", "c74189fc7708f42eea476b3572f624096283b832b082e60432ee620969a153e6"),
                ("****5918", "19d8e1a307d593d525c4f9b5372b8fc38ea8f9abe31a89359d013c18e21b1c8e"),
                ("****1892", "f343a79ac41d584201b7a5bc9536c503b876de1bed2b528602c1cd55b141c660"),
            ).toDF("original_value", "hashed_value").collect()
            extractedPersonalInformationDf.show(false)
            actualExtractedPiiData.length should equal(expected.length)
            actualExtractedPiiData should contain theSameElementsAs expected
        }
        // check hashed df
        val actualFieldsReturnedForHashedDf = hashedOriginalDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for hashed data frame:\n" + hashedOriginalDf.schema.treeString) {
            actualFieldsReturnedForHashedDf shouldEqual Array("record_value", "some_other_top_level_column")
        }
        val actualHashedOriginalDfData = hashedOriginalDf.collect()
        withClue("Actual hashed data frame data do not match") {
            val expected = List(
                (s"""{"pii_credit_card_column":"c74189fc7708f42eea476b3572f624096283b832b082e60432ee620969a153e6","pii_customer_name":"818b5cc5f21d3e6e4e6071c06294528d44595022218446d8b79304d2b766327a","some_other_col_inside_json_object":9}""", 40),
                (s"""{"pii_credit_card_column":"19d8e1a307d593d525c4f9b5372b8fc38ea8f9abe31a89359d013c18e21b1c8e","pii_customer_name":"3d28271ec52e3d07fe14f5f16d01f2c09cbcac1949f9904b305136d0edbee12d"}""", 50),
                (s"""{"pii_credit_card_column":"f343a79ac41d584201b7a5bc9536c503b876de1bed2b528602c1cd55b141c660","pii_customer_name":"1ec3fb2651897b571d646d06b431e5deb7de3a9ac2b283ffa124c5a20805f501"}""", 60),
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }
    }

    test("test json hash with transform function") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map(
            "pii_columns" -> List(
                Map("outerCol" -> "record_value",
                    "innerCol" -> Map("type" -> "jsonPath", "jsonPath" -> "$.pii_credit_card_column")),
                Map("outerCol" -> "record_value",
                    "innerCol" -> Map("type" -> "jsonPath", "jsonPath" -> "$.email_address"),
                    "transform" -> Map("transformationName" -> "normalizeEmail")
                )))
        val jsonHashingFunction = new JsonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            (s"""{"pii_credit_card_column": "****1234", "email_address" : "PauL@Mailbox.Com", "some_other_col_inside_json_object" : 9}""", 40),
            (s"""{"pii_credit_card_column": "****5918", "email_address" : "GEORGE@mailbox.COM"}""", 50),
            (s"""{"pii_credit_card_column": "****1892", "email_address" : "rinGO@mailboX.com"}""", 60),
        ).toDF("record_value", "some_other_top_level_column")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = jsonHashingFunction.hash(landingData)

        // then
        // check pii df
        val actualFieldsReturnedForPiiDf = extractedPersonalInformationDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for pii data frame:\n" + extractedPersonalInformationDf.schema.treeString) {
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                ("paul@mailbox.com", "c6d350e268541121fe992734ce63ae64288a0dd46ec716991dbf6809eada1e91"),
                ("george@mailbox.com", "f13d2bcb9f2cf5a3818ceb4ac26df50fef3a24d8565f18fb23b76a2514480f3c"),
                ("ringo@mailbox.com", "e78b37db08be40533170866d8c275c9a56bb85188fb92ddaf7a77dee9ec0c877"),
                ("****1234", "c74189fc7708f42eea476b3572f624096283b832b082e60432ee620969a153e6"),
                ("****5918", "19d8e1a307d593d525c4f9b5372b8fc38ea8f9abe31a89359d013c18e21b1c8e"),
                ("****1892", "f343a79ac41d584201b7a5bc9536c503b876de1bed2b528602c1cd55b141c660"),
            ).toDF("original_value", "hashed_value").collect()
            extractedPersonalInformationDf.show(false)
            actualExtractedPiiData.length should equal(expected.length)
            actualExtractedPiiData should contain theSameElementsAs expected
        }
        // check hashed df
        val actualFieldsReturnedForHashedDf = hashedOriginalDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for hashed data frame:\n" + hashedOriginalDf.schema.treeString) {
            actualFieldsReturnedForHashedDf shouldEqual Array("record_value", "some_other_top_level_column")
        }
        val actualHashedOriginalDfData = hashedOriginalDf.collect()
        withClue("Actual hashed data frame data do not match") {
            val expected = List(
                (s"""{"pii_credit_card_column":"c74189fc7708f42eea476b3572f624096283b832b082e60432ee620969a153e6","email_address":"c6d350e268541121fe992734ce63ae64288a0dd46ec716991dbf6809eada1e91","some_other_col_inside_json_object":9}""", 40),
                (s"""{"pii_credit_card_column":"19d8e1a307d593d525c4f9b5372b8fc38ea8f9abe31a89359d013c18e21b1c8e","email_address":"f13d2bcb9f2cf5a3818ceb4ac26df50fef3a24d8565f18fb23b76a2514480f3c"}""", 50),
                (s"""{"pii_credit_card_column":"f343a79ac41d584201b7a5bc9536c503b876de1bed2b528602c1cd55b141c660","email_address":"e78b37db08be40533170866d8c275c9a56bb85188fb92ddaf7a77dee9ec0c877"}""", 60),
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }
    }

    // TODO: add support (if needed) for other pii data types, not only strings
    ignore("test json hash on numeric pii columns") {

    }
    ignore("test json hash on boolean pii columns") {
    }
}

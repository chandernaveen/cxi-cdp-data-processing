package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class CommonHashingFunctionTest extends BaseSparkBatchJobTest {

    test("test hashing of top level column") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map("dataColName" -> "record_value")
        val commonHashingFunction = new CommonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            ("Bob", 10),
            ("Alice", 20),
            ("John", 30)
        ).toDF("record_value", "some_other_top_level_column")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = commonHashingFunction.hash(landingData)

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
                ("cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961", 10),
                ("3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043", 20),
                ("a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da", 30)
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }

    }

}

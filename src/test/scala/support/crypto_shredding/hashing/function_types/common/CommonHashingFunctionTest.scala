package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

import support.BaseSparkBatchJobTest

import com.cxi.cdp.data_processing.support.crypto_shredding.hashing.function_types.CryptoHashingResult
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class CommonHashingFunctionTest extends BaseSparkBatchJobTest {

    test("test hashing of top level column") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map("pii_columns" -> Seq(Map("column" -> "record_value")))
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
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value", "identity_type")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                CryptoHashingResult("Bob", "cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961", null),
                CryptoHashingResult("Alice", "3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043", null),
                CryptoHashingResult("John", "a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da", null)
            )
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

    test("test hashing of multiple top level columns") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map("pii_columns" -> Seq(Map("column" -> "record_value"), Map("column" -> "some_other_top_level_column")))
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
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value", "identity_type")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                CryptoHashingResult("Bob", "cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961", null),
                CryptoHashingResult("Alice", "3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043", null),
                CryptoHashingResult("John", "a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da", null),
                CryptoHashingResult("10", "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5", null),
                CryptoHashingResult("20", "f5ca38f748a1d6eaf726b8a42fb575c3c71f1864a8143301782de13da2d9202b", null),
                CryptoHashingResult("30", "624b60c58c9d8bfb6ff1886c2fd605d2adeb6ea4da576068201b6c6958ce93f4", null)
            )
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
                ("cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961", "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5"),
                ("3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043", "f5ca38f748a1d6eaf726b8a42fb575c3c71f1864a8143301782de13da2d9202b"),
                ("a8cfcd74832004951b4408cdb0a5dbcd8c7e52d43f7fe244bf720582e05241da", "624b60c58c9d8bfb6ff1886c2fd605d2adeb6ea4da576068201b6c6958ce93f4")
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }

    }

    test("test hashing with transform function") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map("pii_columns" -> Seq(Map("column" -> "email_address", "transform" -> Map("transformationName" -> "normalizeEmail"), "identity_type" -> "email")))
        val commonHashingFunction = new CommonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            ("Paul", 10, "PauL@Mailbox.Com"),
            ("George", 20, "GEORGE@mailbox.COM"),
            ("Ringo", 30, "rinGO@mailboX.com")
        ).toDF("record_value", "some_other_top_level_column", "email_address")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = commonHashingFunction.hash(landingData)

        // then
        // check pii df
        val actualFieldsReturnedForPiiDf = extractedPersonalInformationDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for pii data frame:\n" + extractedPersonalInformationDf.schema.treeString) {
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value", "identity_type")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                CryptoHashingResult("paul@mailbox.com", "c6d350e268541121fe992734ce63ae64288a0dd46ec716991dbf6809eada1e91", "email"),
                CryptoHashingResult("george@mailbox.com", "f13d2bcb9f2cf5a3818ceb4ac26df50fef3a24d8565f18fb23b76a2514480f3c", "email"),
                CryptoHashingResult("ringo@mailbox.com", "e78b37db08be40533170866d8c275c9a56bb85188fb92ddaf7a77dee9ec0c877", "email")
            )
            actualExtractedPiiData.length should equal(expected.length)
            actualExtractedPiiData should contain theSameElementsAs expected
        }
        // check hashed df
        val actualFieldsReturnedForHashedDf = hashedOriginalDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for hashed data frame:\n" + hashedOriginalDf.schema.treeString) {
            actualFieldsReturnedForHashedDf shouldEqual Array("record_value", "some_other_top_level_column", "email_address")
        }
        val actualHashedOriginalDfData = hashedOriginalDf.collect()
        withClue("Actual hashed data frame data do not match") {
            val expected = List(
                ("Paul", 10, "c6d350e268541121fe992734ce63ae64288a0dd46ec716991dbf6809eada1e91"),
                ("George", 20, "f13d2bcb9f2cf5a3818ceb4ac26df50fef3a24d8565f18fb23b76a2514480f3c"),
                ("Ringo", 30, "e78b37db08be40533170866d8c275c9a56bb85188fb92ddaf7a77dee9ec0c877")
            ).toDF("record_value", "some_other_top_level_column", "email_address").collect()
            actualHashedOriginalDfData.length should equal(expected.length)
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }
    }

    test("test filtering of empty values") {
        // given
        import spark.implicits._
        val salt = ""
        val hashFunctionConfig = Map("pii_columns" -> Seq(Map("column" -> "record_value")))
        val commonHashingFunction = new CommonHashingFunction(hashFunctionConfig, salt)
        val landingData = List(
            ("Bob", 10),
            (null, 20),
            (null, 30)
        ).toDF("record_value", "some_other_top_level_column")

        // when
        val (hashedOriginalDf, extractedPersonalInformationDf) = commonHashingFunction.hash(landingData)

        // then
        // check pii df
        val actualFieldsReturnedForPiiDf = extractedPersonalInformationDf.schema.fields.map(f => f.name)
        withClue("Actual fields returned for pii data frame:\n" + extractedPersonalInformationDf.schema.treeString) {
            actualFieldsReturnedForPiiDf shouldEqual Array("original_value", "hashed_value", "identity_type")
        }
        val actualExtractedPiiData = extractedPersonalInformationDf.collect()
        withClue("Actual extracted PII data do not match") {
            val expected = List(
                CryptoHashingResult("Bob", "cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961", null)
            )
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
                (null, 20),
                (null, 30),
            ).toDF("record_value", "some_other_top_level_column").collect()
            actualHashedOriginalDfData should contain theSameElementsAs expected
        }

    }

}

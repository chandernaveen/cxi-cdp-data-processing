package com.cxi.cdp.data_processing
package refined_zone.throtle.service

import refined_zone.throtle.model.TransformedField
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{convertToAnyShouldWrapper, the}

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer

class ThrotleValidatorTest extends BaseSparkBatchJobTest {

    case class CodeCategoryValidationTestCase
        (allColumnNames: ListSet[String], booleanColumnNames: ListSet[String], doNotTransformColumnNames: ListSet[String], integerColumnNames: ListSet[String],
         dictionaryColumnNames: ListSet[String], error: String)

    test("Validate code category - validation passed") {
        // when
        ThrotleValidator.validateColumnConfigurationIsPresent(
            allColumnNames = Set("A", "B", "C", "D", "E"),
            booleanColumnNames = Set("A", "B"),
            doNotTransformColumnNames = Set("C"),
            dictionaryColumnNames = Set("D"),
            integerColumnNames = Set("E")
        )
        // then
        // no exception
    }

    test("Validate code category - validation failed") {
        // given
        val testCases = Seq(
            CodeCategoryValidationTestCase(ListSet("A", "B", "C", "D", "E"), ListSet("A"), ListSet("C"), ListSet("D"), ListSet("E"), "B"),
            CodeCategoryValidationTestCase(ListSet("A", "B", "C", "D", "E"), ListSet("A", "B"), ListSet(), ListSet("D"), ListSet(), "C, E"),
            CodeCategoryValidationTestCase(ListSet("A", "B", "C", "D", "E"), ListSet("A", "B"), ListSet("C"), ListSet(), ListSet("E"), "D"),
            CodeCategoryValidationTestCase(ListSet("A", "B", "C", "D", "E"), ListSet(), ListSet(), ListSet(), ListSet(), "A, B, C, D, E")
        )

        // when
        for (testCase <- testCases) {
            val exception = the[IllegalArgumentException] thrownBy
                ThrotleValidator.validateColumnConfigurationIsPresent(testCase.allColumnNames, testCase.booleanColumnNames, testCase.doNotTransformColumnNames,
                    testCase.integerColumnNames, testCase.dictionaryColumnNames)

            // then
            val errorMsg = s"""Configuration not found for columns in raw dataset: ${testCase.error}.
                Please make sure the field is present in any of the following:
                1. 'schema.do_not_transform_columns' property
                2. 'schema.boolean_columns' property
                3. 'schema.integer_columns' property
                4. throtle dictionary"""
            exception.getMessage shouldBe errorMsg
        }
    }

    test("Validate code in dictionary - validation passed") {
        // given
        import spark.implicits._
        val transformedDf = Seq(
            (TransformedField("income", "A", "Under $10K", true), TransformedField("occupation", "1", "Homemaker", true)),
            (TransformedField("income", null, null, true), TransformedField("occupation", null, null, true))
        ).toDF("income", "occupation")

        // when
        ThrotleValidator.validateCodeIsPresentInDictionary(transformedDf, ArrayBuffer("income", "occupation"), spark)
        // then
        // no exception
    }

    test("Validate code in dictionary - validation failed") {
        // given
        import spark.implicits._
        val transformedDf = Seq(
            (TransformedField("income", "A", "Under $10K", true), TransformedField("occupation", "1", "Homemaker", true)),
            (TransformedField("income", null, null, true), TransformedField("occupation", null, null, true)),
            (TransformedField("income", "XYZ", null, false), TransformedField("occupation", null, null, true)),
            (TransformedField("income", "XYZ", null, false), TransformedField("occupation", "ZXC", null, false)),
            (TransformedField("income", "A", "Under $10K", true), TransformedField("occupation", "ZXC", null, false))
        ).toDF("income", "occupation")

        // when
        val exception = the[IllegalArgumentException] thrownBy
            ThrotleValidator.validateCodeIsPresentInDictionary(transformedDf, ArrayBuffer("income", "occupation"), spark)

        // then
        val errorMsg = s"""Code values are missing from the throtle dictionary: (show first 100 records):
                          |({code_category: income, code: XYZ}, {code_category: occupation, code: ZXC})""".stripMargin
        exception.getMessage shouldBe errorMsg
    }

}

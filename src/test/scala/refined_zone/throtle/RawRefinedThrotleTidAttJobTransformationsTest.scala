package com.cxi.cdp.data_processing
package refined_zone.throtle

import refined_zone.throtle.model.TransformedField

import org.scalatest.FunSuite
import org.scalatest.Matchers.{convertToAnyShouldWrapper, the}

import java.lang.Boolean.{FALSE, TRUE}

class RawRefinedThrotleTidAttJobTransformationsTest extends FunSuite {

    case class ParseBooleanTestCase(value: String, expected: java.lang.Boolean)
    case class CastToIntTestCase(value: String, expected: java.lang.Integer)
    case class ThrotleCodeValueTestCase(code_category: String, code: String, expected: TransformedField)

    test("Parse boolean field") {
        // given
        val testCases = Seq(
            ParseBooleanTestCase("1", TRUE),
            ParseBooleanTestCase("y", TRUE),
            ParseBooleanTestCase("Y", TRUE),
            ParseBooleanTestCase("Yes", TRUE),
            ParseBooleanTestCase("yes", TRUE),
            ParseBooleanTestCase("YES", TRUE),
            ParseBooleanTestCase("true", TRUE),
            ParseBooleanTestCase("True", TRUE),
            ParseBooleanTestCase("t", TRUE),
            ParseBooleanTestCase("U", null),
            ParseBooleanTestCase("Unknown", null),
            ParseBooleanTestCase("False", FALSE),
            ParseBooleanTestCase("FALSE", FALSE),
            ParseBooleanTestCase("0", FALSE),
            ParseBooleanTestCase("n", FALSE),
            ParseBooleanTestCase("N", FALSE),
            ParseBooleanTestCase("No", FALSE),
            ParseBooleanTestCase("NO", FALSE),
            ParseBooleanTestCase("f", FALSE),
            ParseBooleanTestCase(null, null)
        )

        // when
        for (testCase <- testCases) {
            RawRefinedThrotleTidAttJob.parseBooleanField(testCase.value) shouldEqual testCase.expected
        }
    }

    test("Unable to parse boolean field") {
        // given
        val testValues = Seq("42", "xyz", "yes_flag")

        for (value <- testValues) {
            val exception = the[IllegalArgumentException] thrownBy RawRefinedThrotleTidAttJob.parseBooleanField(value)

            // then
            val errorMsg = s"Unable to convert to Bollean: '$value'"
            exception.getMessage shouldBe errorMsg
        }
    }

    test("Get throtle code value") {
        // given
        val dict = Map(
            "income" -> Map(
                "A" -> "Under $10K",
                "B" -> "10-19,999",
                "C" -> "20-29,999",
                "D" -> "30-39,999"
            ),
            "occupation" -> Map(
                "1" -> "Homemaker",
                "2" -> "Professional/technical"
            )
        )

        val testCases = Seq(
            ThrotleCodeValueTestCase("income", "A", TransformedField("income", "A", "Under $10K", true)),
            ThrotleCodeValueTestCase("income", "B", TransformedField("income", "B", "10-19,999", true)),
            ThrotleCodeValueTestCase("income", "C", TransformedField("income", "C", "20-29,999", true)),
            ThrotleCodeValueTestCase("income", "D", TransformedField("income", "D", "30-39,999", true)),
            ThrotleCodeValueTestCase("income", "XYZ", TransformedField("income", "XYZ", null, false)),
            ThrotleCodeValueTestCase("income", null, TransformedField("income", null, null, true)),
            ThrotleCodeValueTestCase("occupation", "1", TransformedField("occupation", "1", "Homemaker", true)),
            ThrotleCodeValueTestCase("occupation", "2", TransformedField("occupation", "2", "Professional/technical", true)),
            ThrotleCodeValueTestCase("occupation", "ZXC", TransformedField("occupation", "ZXC", null, false)),
            ThrotleCodeValueTestCase("occupation", null, TransformedField("occupation", null, null, true)),
            ThrotleCodeValueTestCase("some_unknown_category", null, TransformedField("some_unknown_category", null, null, true))
        )

        // when
        for (testCase <- testCases) {
            RawRefinedThrotleTidAttJob.getThrotleTransformedField(testCase.code_category, testCase.code, dict) shouldEqual testCase.expected
        }
    }

    test("Cast to Integer") {
        // given
        val testCases = Seq(
            CastToIntTestCase("42", 42),
            CastToIntTestCase(null, null),
            CastToIntTestCase("0", 0)
        )

        // when
        for (testCase <- testCases) {
            RawRefinedThrotleTidAttJob.castToInt("some_column", testCase.value) shouldEqual testCase.expected
        }
    }
}

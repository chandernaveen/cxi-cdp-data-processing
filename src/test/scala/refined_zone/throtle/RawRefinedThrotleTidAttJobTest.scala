package com.cxi.cdp.data_processing
package refined_zone.throtle

import support.BaseSparkBatchJobTest

import org.apache.spark.SparkException
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, the}

import java.lang.Boolean.{FALSE, TRUE}
import scala.collection.immutable.ListSet

class RawRefinedThrotleTidAttJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("Process to refined DF") {

        // given
        val feedDate = "2022-02-24"
        val booleanFields = ListSet("travel")
        val doNotTransformFields = ListSet("throtle_id", "throtle_hhid", "dob")
        val integerFields = ListSet("age")

        val throtleDictTable = "temp_throtle_dict"
        val throtle_dict = Seq(
            ("occupation_code", "B110", "Engineer/Civil"),
            ("occupation_code", "B111", "Engineer/Electrical/Electronic"),
            ("income", "A", "Under $10K"),
            ("income", "B", "10-19,999"),
            ("income", "C", "20-29,999")
        ).toDF("code_category", "code", "code_value")
        throtle_dict.createOrReplaceTempView(throtleDictTable)

        val rawTable = "temp_tid_att"
        val tid_att_raw = Seq(
            // should dedup
            ("thr_id_01", "thr_hh_id_11", "A", "B110", "2001-05", null, "25", feedDate),
            ("thr_id_01", "thr_hh_id_11", "A", "B110", "2001-05", null, "25", feedDate),

            ("thr_id_02", "thr_hh_id_22", "B", "B111", "2008-07", "Y", null, feedDate),
            ("thr_id_03", "thr_hh_id_22", "C", null, "2009-01", "unKnoWN", "41", feedDate),
            ("thr_id_04", "thr_hh_id_44", "A", "B111", "2009-01", "1", "17", feedDate),

            // another feed_date, should ignore
            ("thr_id_05", "thr_hh_id_55", "A", "B110", "2009-01", "1", "25", "2021-01-01")

        ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "dob", "travel", "age", "feed_date")
        tid_att_raw.createOrReplaceTempView(rawTable)

        // when
        val actual = RawRefinedThrotleTidAttJob.process(rawTable, throtleDictTable, booleanFields, doNotTransformFields, integerFields, feedDate, spark)

        // then
        withClue("Refined tid_att do not match") {
            val expected = Seq(
                ("thr_id_01", "thr_hh_id_11", "Under $10K", "Engineer/Civil", "2001-05", null, new Integer(25)),
                ("thr_id_02", "thr_hh_id_22", "10-19,999", "Engineer/Electrical/Electronic", "2008-07", TRUE, null),
                ("thr_id_03", "thr_hh_id_22", "20-29,999", null, "2009-01", null, new Integer(41)),
                ("thr_id_04", "thr_hh_id_44", "Under $10K", "Engineer/Electrical/Electronic", "2009-01", TRUE, new Integer(17))
            ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "dob", "travel", "age")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Exception if code value is missing") {

        // given
        val feedDate = "2022-02-24"
        val booleanFields = ListSet("travel")
        val doNotTransformFields = ListSet("throtle_id", "throtle_hhid", "dob")
        val integerFields = ListSet("age")

        val throtleDictTable = "temp_throtle_dict"
        val throtle_dict = Seq(
            ("occupation_code", "B110", "Engineer/Civil"),
            ("occupation_code", "B111", "Engineer/Electrical/Electronic"),
            ("income", "A", "Under $10K"),
            ("income", "B", "10-19,999"),
            ("income", "C", "20-29,999")
        ).toDF("code_category", "code", "code_value")
        throtle_dict.createOrReplaceTempView(throtleDictTable)

        val rawTable = "temp_tid_att"
        val tid_att_raw = Seq(

            ("thr_id_01", "thr_hh_id_11", "AA", "B110", "2001-05", null, "13", feedDate), // income 'AA' is missing
            ("thr_id_01", "thr_hh_id_11", "A", "B110", "2001-05", null, "34", feedDate), // valid row

            ("thr_id_02", "thr_hh_id_22", "B", "B1111", "2008-07", "Y", "22", feedDate), // occupation_code 'B1111' is missing
            ("thr_id_03", "thr_hh_id_22", "C", null, "2009-01", "xyz", "58", feedDate), // valid row
            ("thr_id_04", "thr_hh_id_44", "xyz", "XYZ", "2009-01", "1", "33", feedDate) // income 'xyz' and occupation_code 'XYZ' are missing

        ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "dob", "travel", "age", "feed_date")
        tid_att_raw.createOrReplaceTempView(rawTable)

        // when
        val exception = the[IllegalArgumentException] thrownBy RawRefinedThrotleTidAttJob.process(
            rawTable, throtleDictTable, booleanFields, doNotTransformFields, integerFields, feedDate, spark)

        // then
        val errorMsg =
            """Code values are missing from the throtle dictionary: (show first 100 records):
              |({code_category: income, code: AA}, {code_category: occupation_code, code: B1111}, {code_category: income, code: xyz}, {code_category: occupation_code, code: XYZ})""".stripMargin

        exception.getMessage shouldBe errorMsg
    }

    test("Exception if code category is missing") {
        // given
        val feedDate = "2022-02-24"
        val booleanFields = ListSet("travel")
        val doNotTransformFields = ListSet("throtle_id", "throtle_hhid", "dob")
        val integerFields = ListSet("age")

        val throtleDictTable = "temp_throtle_dict"
        val throtle_dict = Seq(
            ("occupation_code", "B110", "Engineer/Civil"),
            ("occupation_code", "B111", "Engineer/Electrical/Electronic"),
            ("income", "A", "Under $10K"),
            ("income", "B", "10-19,999"),
            ("income", "C", "20-29,999")
        ).toDF("code_category", "code", "code_value")
        throtle_dict.createOrReplaceTempView(throtleDictTable)

        val rawTable = "temp_tid_att"
        val tid_att = Seq(
            ("thr_id_01", "thr_hh_id_11", "A", "B110", "2001-05", null, "A", null, "24", feedDate)
        // 'some_field' and 'xyz' are absent from configuration or dictionary
        ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "dob", "travel", "some_field", "xyz", "age", "feed_date")
        tid_att.createOrReplaceTempView(rawTable)

        // when
        val exception = the[IllegalArgumentException] thrownBy RawRefinedThrotleTidAttJob.process(
            rawTable, throtleDictTable, booleanFields, doNotTransformFields, integerFields, feedDate, spark)

        // then
        val errorMsg = """Configuration not found for columns in raw dataset: some_field, xyz.
                Please make sure the field is present in any of the following:
                1. 'schema.do_not_transform_columns' property
                2. 'schema.boolean_columns' property
                3. 'schema.integer_columns' property
                4. throtle dictionary"""
        exception.getMessage shouldBe errorMsg
    }

    test("Exception if cannot cast value to Integer") {

        // given
        val feedDate = "2022-02-24"
        val booleanFields = ListSet("travel")
        val doNotTransformFields = ListSet("throtle_id", "throtle_hhid", "dob")
        val integerFields = ListSet("age")

        val throtleDictTable = "temp_throtle_dict"
        val throtle_dict = Seq(
            ("occupation_code", "B110", "Engineer/Civil"),
            ("income", "A", "Under $10K")
        ).toDF("code_category", "code", "code_value")
        throtle_dict.createOrReplaceTempView(throtleDictTable)

        val rawTable = "temp_tid_att"
        val tid_att_raw = Seq(

            ("thr_id_01", "thr_hh_id_11", "A", "B110", "2001-05", "1", "34", feedDate), // valid row
            ("thr_id_02", "thr_hh_id_22", "A", "B110", "2011-06", "1", "age34", feedDate) // 'age34' cannot be cast to Integer

        ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "dob", "travel", "age", "feed_date")
        tid_att_raw.createOrReplaceTempView(rawTable)

        // when
        val exception = the[SparkException] thrownBy RawRefinedThrotleTidAttJob.process(
            rawTable, throtleDictTable, booleanFields, doNotTransformFields, integerFields, feedDate, spark).show()

        // then
        exception.getCause.getCause.getMessage shouldBe "Column 'age' contains value that cannot be cast to Integer: 'age34'"
    }

    test("Transform raw columns") {
        // given
        val dict = Map(
            "income" -> Map("A" -> "Under $10K", "B" -> "10-19,999", "C" -> "20-29,999"),
            "occupation" -> Map("1" -> "Homemaker", "2" -> "Professional/technical"),
            "gender" -> Map("M" -> "Male", "F" -> "Female")
        )

        val rawData = Seq(
            ("thr_id_01", "A", "1", "2001-05", "1", "M", "23"),
            ("thr_id_02", "B", "2", "2006-02", null, "F", "18"),
            ("thr_id_03", null, null, null, "false", null, null),
            ("thr_id_04", "C", "1", null, "Y", "F", "62"),
            ("thr_id_05", null, null, null, null, null, null),
        ).toDF("throtle_id", "income", "occupation", "dob", "travel", "gender", "age")

        // 'gender' field is present in dictionary, but shall not be transformed
        val doNotTransformFields = ListSet("throtle_id", "dob", "gender")
        val booleanFields = ListSet("travel")
        val integerFields = ListSet("age")

        // when
        val actual = RawRefinedThrotleTidAttJob.transform(
            rawData, doNotTransformFields, booleanFields, integerFields, dict, rawData.schema.fieldNames.to[ListSet], spark)

        // then
        withClue("Transformed tid_att data do not match") {
            val expected = Seq(
                ("thr_id_01", "Under $10K", "Homemaker", "2001-05",  TRUE, "M", new Integer(23)),
                ("thr_id_02", "10-19,999", "Professional/technical", "2006-02", null, "F", new Integer(18)),
                ("thr_id_03", null, null, null, FALSE, null, null),
                ("thr_id_04", "20-29,999", "Homemaker", null, TRUE, "F", new Integer(62)),
                ("thr_id_05", null, null, null, null, null, null),
            ).toDF("throtle_id", "income", "occupation", "dob", "travel", "gender", "age")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}

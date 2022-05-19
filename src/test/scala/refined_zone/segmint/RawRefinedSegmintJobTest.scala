package com.cxi.cdp.data_processing
package refined_zone.segmint


import support.BaseSparkBatchJobTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class RawRefinedSegmintJobTest extends BaseSparkBatchJobTest {

    test("test segmint read") {
        // given
        import spark.implicits._
        val rawStruct = new StructType()
          .add("record_type", StringType)
          .add("record_value", StringType)
          .add("feed_date", StringType)
          .add("file_name", StringType)
          .add("cxi_id", StringType)

        val rawSourceData = Seq(
            Row("zip_merch",
            """"2021-02"|"CHEESECAKE FACTORY"|"FOOD AND DINING"|"AMERICAN RESTAURANTS (UNITED STATES)"|""|""|99|105|6265.82|59.67""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1a"), //No postal code
            Row("zip_merch",
            """"2021-02"|"STARBUCKS"|"FOOD AND DINING"|"COFFEE SHOPS"|"AZ"|"2762"|10|10|1209.59|10.25""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1b"),
            Row("zip_merch",
            """"2021-02"|"MCDONALD'S"|"FOOD AND DINING"|"FAST FOOD RESTAURANTS"|"AZ"|"2762"|10|10|42307.88|15.32""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1c"),
            Row("zip_merch",
            """"2021-02"|"JOLLIBEE"|"FOOD AND DINING"|"FAST FOOD RESTAURANTS"|"AZ"|"2762"|10|10|4537.31|22.80""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1d"),
            Row("not_zip",
            """"2021-02"|"JOLLIBEE"|"FOOD AND DINING"|"FAST FOOD RESTAURANTS"|"AZ"|"2762"|184|199|4537.31|22.80""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1d"), //Wrong record type
            Row("zip_merch",
             """"2021-02"|"JACK IN THE BOX"|"FOOD AND DINING"|""|"AZ"|"2762"|144|171|2597.93|15.19""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1e") //No location type
        )

        import collection.JavaConverters._
        val rawSource = spark.createDataFrame(rawSourceData.asJava, rawStruct)
        val tempTableName = "testRawSegmint"

        rawSource.createOrReplaceTempView(tempTableName)

        // when
        val actual = RawRefinedSegmintJob.readSegmint(spark, "2022-02-06", tempTableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("date", "merchant", "location_type", "state", "postal_code", "transaction_quantity", "transaction_amount")
        }
        val actualSegmintRawData = actual.collect()
        withClue("Segmint refined data source do not match") {
            val expected = List(
                ("2021-01-15", "STARBUCKS", "COFFEE SHOPS", "AZ", "2762", 10, 1209.59),
                ("2021-01-15", "MCDONALD'S", "FAST FOOD RESTAURANTS", "AZ", "2762", 10, 42307.88),
                ("2021-01-15", "JOLLIBEE", "FAST FOOD RESTAURANTS", "AZ", "2762", 10, 4537.31)
            ).toDF("date", "merchant", "location_type", "state", "postal_code", "transaction_quantity", "transaction_amount").collect()
            actualSegmintRawData.length should equal(expected.length)
            actualSegmintRawData should contain theSameElementsAs expected
        }
    }

    test("test segmint transformation") {
        // given
        import spark.implicits._
        val segmintRawStruct = new StructType()
          .add("date", StringType)
          .add("merchant", StringType)
          .add("location_type", StringType)
          .add("state", StringType)
          .add("postal_code", StringType)
          .add("transaction_quantity", IntegerType)
          .add("transaction_amount", DoubleType)

        val segmintRawSourceData = Seq(
            Row("2021-01-11", "MCDONALD'S", "FAST FOOD RESTAURANTS", "AK", "99504", 10, 2.99),
            Row("2021-01-11", "MCDONALD'S", "FAST FOOD RESTAURANTS", "AK", "99504", 10, 2.99), //Duplicate
            Row("2021-01-18", "MCDONALD'S", "FAST FOOD RESTAURANTS", "AK", "99504", 10, 5.99),
            Row("2021-01-11", "STARBUCKS", "COFFEE SHOPS", "AK", "99503", 5, 5.99),
            Row("2021-01-18", "STARBUCKS", "COFFEE SHOPS", "AK", "99503", 4, 5.99),
            Row("2021-01-25", "STARBUCKS", "COFFEE SHOPS", "AK", "99503", 3, 5.99),
            Row("2021-01-11", "MOOSE'S TOOTH", "PIZZA PARLORS", "AK", "99502", 1, 1.99),
            Row("2021-01-11", "MOOSE'S TOOTH", "PIZZA PARLORS", "AK", "99999", 1, 2.99), //Wrong Postal Code, should drop
            Row("2021-01-11", "TACO BELL", "MEXICAN RESTAURANTS", "AK", "99504", 2, 3.00)
        )

        import collection.JavaConverters._
        val segmintRawSource = spark.createDataFrame(segmintRawSourceData.asJava, segmintRawStruct)

        val postalCodeStruct = new StructType()
          .add("postal_code", StringType)
          .add("city", StringType)
          .add("region", StringType)

        val postalCodeRawSourceData = Seq(
            Row("99504", "City 1", "Region 1"),
            Row("99503", "City 2", "Region 1"),
            Row("99502", "city 3", "Region 1") //Fix City 3
            )

        val postalCodeRawSource = spark.createDataFrame(postalCodeRawSourceData.asJava, postalCodeStruct)

        // when
        val actual = RawRefinedSegmintJob.transformSegmint(segmintRawSource, postalCodeRawSource)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("date", "merchant", "location_type", "state", "postal_code", "transaction_quantity", "transaction_amount", "city", "region")
        }
        val actualSegmintRefinedData = actual.collect()
        withClue("Segmint refined data transform do not match") {
            val expected = List(
                ("2021-01-11", "MCDONALD'S", "FAST FOOD RESTAURANTS", "AK", "99504", 10, 2.99, "City 1", "Region 1"),
                ("2021-01-18", "MCDONALD'S", "FAST FOOD RESTAURANTS", "AK", "99504", 10, 5.99, "City 1", "Region 1"),
                ("2021-01-11", "STARBUCKS", "COFFEE SHOPS", "AK", "99503", 5, 5.99, "City 2", "Region 1"),
                ("2021-01-18", "STARBUCKS", "COFFEE SHOPS", "AK", "99503", 4, 5.99, "City 2", "Region 1"),
                ("2021-01-25", "STARBUCKS", "COFFEE SHOPS", "AK", "99503", 3, 5.99, "City 2", "Region 1"),
                ("2021-01-11", "MOOSE'S TOOTH", "PIZZA PARLORS", "AK", "99502", 1, 1.99, "City 3", "Region 1"),
                ("2021-01-11", "TACO BELL", "MEXICAN RESTAURANTS", "AK", "99504", 2, 3.00, "City 1", "Region 1")
            ).toDF("date", "merchant", "location_type", "state", "postal_code", "transaction_quantity", "transaction_amount", "city", "region").collect()
            actualSegmintRefinedData.length should equal(expected.length)
            actualSegmintRefinedData should contain theSameElementsAs expected
        }
    }

    test("convert YYYY-WW date to ISO8601 format") {
        // given
        val testCases = Seq(
            DateConverterTestCase("2022-01", "2022-01-07"),
            DateConverterTestCase("2022-02", "2022-01-14"),
            DateConverterTestCase("2022-11", "2022-03-18"),
            DateConverterTestCase("2022-53", "2023-01-06"),
            DateConverterTestCase("2023-01", "2023-01-13"),
            DateConverterTestCase("2023-52", "2024-01-05")
        )

        for (testCase <- testCases) {
            // when
            val actual = RawRefinedSegmintJob.convertYearWeekToIso8601Date(testCase.yearWeekDate)
            // then
            actual should equal(testCase.converted)
        }
    }

    case class DateConverterTestCase(yearWeekDate: String, converted: String)
}

package com.cxi.cdp.data_processing
package curated_zone.tmi

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class TotalMarketInsightsFromSegmintJobTest extends BaseSparkBatchJobTest {

    test("test total market insights read") {
        // given
        import spark.implicits._
        val totalMarketInsightRefinedStructure = new StructType()
          .add("date", StringType)
          .add("merchant", StringType)
          .add("location_type", StringType)
          .add("state", StringType)
          .add("postal_code", StringType)
          .add("transaction_quantity", IntegerType)
          .add("transaction_amount", DoubleType)
          .add("city", StringType)
          .add("region", StringType)

        val totalMarketInsightRefinedData = Seq(
            Row("2022-02-06", "Example 1", "RESTAURANTS", "AZ", "1234", 2, 2.99, "City 1", "Region 1"),
            Row("2022-02-06", "Example 1", "RESTAURANTS", "AZ", "1234", 2, 2.99, "City 1", "Region 1"),
            Row("2022-02-06", "Example 1", "DINER", "AZ", "1234", 2, 2.99, "City 1", "Region 1"),
            Row("2022-02-06", "Example 1", "HOTAL", "AZ", "1234", 2, 2.99, "City 1", "Region 1") //Not Restaurant
        )

        import collection.JavaConverters._
        val totalMarketInsightRefined = spark.createDataFrame(totalMarketInsightRefinedData.asJava, totalMarketInsightRefinedStructure)
        val tempTableName = "testRefinedSegmint"

        totalMarketInsightRefined.createOrReplaceTempView(tempTableName)
        // when
        val actual = TotalMarketInsightsFromSegmintJob.readTotalMarketInsights(tempTableName)(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
        }
        val actualTotalMarketInsightsData = actual.collect()
        withClue("Total market insights data do not match") {
            val expected = List(
                ("RESTAURANTS", "Region 1", "AZ", "City 1", "2022-02-06", 2.99, 2),
                ("RESTAURANTS", "Region 1", "AZ", "City 1", "2022-02-06", 2.99, 2),
                ("DINER", "Region 1", "AZ", "City 1", "2022-02-06", 2.99, 2)
            ).toDF("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity").collect()
            actualTotalMarketInsightsData.length should equal(expected.length)
            actualTotalMarketInsightsData should contain theSameElementsAs expected
        }
    }

    test("test total market insight compute") {
        // given
        import spark.implicits._
        val totalMarketInsightStructure = new StructType()
          .add("location_type", StringType)
          .add("region", StringType)
          .add("state", StringType)
          .add("city", StringType)
          .add("date", StringType)
          .add("transaction_amount", DoubleType)
          .add("transaction_quantity", IntegerType)

        val totalMarketInsightData = Seq(
            Row("RESTAURANTS", "Region 1", "AZ", "City 1", "2022-02-06", 2.99, 2),
            Row("RESTAURANTS", "Region 1", "AZ", "City 1", "2022-02-06", 2.99, 2),
            Row("RESTAURANTS", "Region 1", "AZ", "City 1", "2022-02-07", 2.99, 2),
            Row("DINER", "Region 1", "AZ", "City 1", "2022-02-06", 2.99, 2)
        )

        import collection.JavaConverters._
        val totalMarketInsight = spark.createDataFrame(totalMarketInsightData.asJava, totalMarketInsightStructure)

        // when
        val actual = TotalMarketInsightsFromSegmintJob.transformTotalMarketInsights(totalMarketInsight)(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
        }
        val actualTotalMarketInsightsData = actual.collect()
        withClue("Total market insights data do not match") {
            val expected = List(
                ("Restaurant", "Region 1", "AZ", "City 1", "2022-02-06", 8.97, 6),
                ("Restaurant", "Region 1", "AZ", "City 1", "2022-02-07", 2.99, 2)
            ).toDF("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity").collect()
            actualTotalMarketInsightsData.length should equal(expected.length)
            actualTotalMarketInsightsData should contain theSameElementsAs expected
        }
    }

}
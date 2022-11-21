package com.cxi.cdp.data_processing
package curated_zone.tmi

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class TotalMarketInsightsFromSegmintJobTest extends BaseSparkBatchJobTest {

    test("test total market insights read") {
        // given
        import spark.implicits._
        val totalMarketInsightRefinedStructure = new StructType()
            .add("date", StringType)
            .add("merchant", StringType)
            .add("location_type", StringType)
            .add("industry_category", StringType)
            .add("cuisine_category", StringType)
            .add("state", StringType)
            .add("postal_code", StringType)
            .add("transaction_quantity", IntegerType)
            .add("transaction_amount", DoubleType)
            .add("city", StringType)
            .add("region", StringType)

        val totalMarketInsightRefinedData = Seq(
            Row(
                "2022-09-06",
                "Example 1",
                "RESTAURANTS",
                "Fast Casual",
                "American",
                "AZ",
                "1234",
                2,
                2.99,
                "City 1",
                "Region 1"
            ),
            Row(
                "2022-09-06",
                "Example 1",
                "RESTAURANTS",
                "Fast Casual",
                "American",
                "AZ",
                "1234",
                2,
                2.99,
                "City 1",
                "Region 1"
            ),
            Row(
                "2022-09-06",
                "Example 2",
                "DINER",
                "Casual Dining",
                "American-Diner",
                "AZ",
                "1234",
                2,
                2.99,
                "City 1",
                "Region 1"
            ),
            Row("2022-09-06", "Example 2", "BISTROS", "Cafe", "French", "AZ", "1234", 2, 2.99, "City 1", "Region 1")
        )

        import collection.JavaConverters._
        val totalMarketInsightRefined =
            spark.createDataFrame(totalMarketInsightRefinedData.asJava, totalMarketInsightRefinedStructure)
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
                ("American", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2),
                ("American", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2),
                ("American-Diner", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2),
                ("French", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2)
            ).toDF("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
                .collect()
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
            Row("American", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2),
            Row("American", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2),
            Row("American", "Region 1", "AZ", "City 1", "2022-09-07", 2.99, 2),
            Row("American-Diner", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2)
        )

        // noinspection ScalaStyle
        val totalMarketInsightDataTable = Seq(
            Row("American", "Region 1", "AZ", "City 1", "2022-09-06", 5.98, 4),
            Row("American", "Region 1", "AZ", "City 1", "2022-09-07", 2.99, 2)
        )

        import collection.JavaConverters._
        val totalMarketInsight = spark.createDataFrame(totalMarketInsightData.asJava, totalMarketInsightStructure)

        val totalMarketInsightTable =
            spark.createDataFrame(totalMarketInsightDataTable.asJava, totalMarketInsightStructure)

        // when
        val fullReprocess = true
        val actualFull = TotalMarketInsightsFromSegmintJob.transformTotalMarketInsights(
            totalMarketInsight,
            totalMarketInsightTable,
            fullReprocess
        )(spark)

        val actualDelta = TotalMarketInsightsFromSegmintJob.transformTotalMarketInsights(
            totalMarketInsight,
            totalMarketInsightTable,
            !fullReprocess
        )(spark)

        // then-full
        val actualFullFieldsReturned = actualFull.schema.fields.map(f => f.name)
        withClue("Actual Full Reprocess fields returned:\n" + actualFull.schema.treeString) {
            actualFullFieldsReturned should contain theSameElementsAs
                Seq("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
        }
        val actualFullTotalMarketInsightsData = actualFull.collect()
        withClue("Total market insights Full Reprocess data do not match") {
            val expectedFull = List(
                ("American", "Region 1", "AZ", "City 1", "2022-09-06", 5.98, 4),
                ("American", "Region 1", "AZ", "City 1", "2022-09-07", 2.99, 2),
                ("American-Diner", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2)
            ).toDF("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
                .collect()
            actualFullTotalMarketInsightsData.length should equal(expectedFull.length)
            actualFullTotalMarketInsightsData should contain theSameElementsAs expectedFull
        }
//then delta
        val actualDeltaFieldsReturned = actualDelta.schema.fields.map(f => f.name)
        withClue("Actual Delta process fields returned:\n" + actualDelta.schema.treeString) {
            actualDeltaFieldsReturned should contain theSameElementsAs
                Seq("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
        }
        val actualDeltaTotalMarketInsightsData = actualDelta.collect()
        withClue("Total market insights Delta data do not match") {
            val expectedDelta = List(
                ("American-Diner", "Region 1", "AZ", "City 1", "2022-09-06", 2.99, 2)
            ).toDF("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
                .collect()
            actualDeltaTotalMarketInsightsData.length should equal(expectedDelta.length)
            actualDeltaTotalMarketInsightsData should contain theSameElementsAs expectedDelta
        }
    }

}

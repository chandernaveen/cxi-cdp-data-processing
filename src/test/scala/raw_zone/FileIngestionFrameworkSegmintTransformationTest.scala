package com.cxi.cdp.data_processing
package raw_zone

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import org.scalatest.Matchers

class FileIngestionFrameworkSegmintTransformationTest extends BaseSparkBatchJobTest with Matchers {

    test("test Segmint transformation with raw data") {
        // given
        import sqlContext.implicits._
        val landingRawStruct = new StructType()
          .add("cursor", StringType)
          .add("feed_date", StringType)
          .add("file_name", StringType)
          .add("cxi_id", StringType)

        val landingRawSourceData = Seq(
            Row(""""2021-02"|"FOOD AND DINING"|"JAPANESE NOODLE RESTAURANTS"|""|"AGE: 0-20"|1|1|41.59|41.59""",
            "2022-02-06", "202102_cxi_age_category.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1a"),
            Row(""""2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
            "2022-02-06", "202102_cxi_age_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1b"),
            Row(""""2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1c"),
            Row(""""2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1d"),
            Row(""""2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
            "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1e")
        )

        import collection.JavaConverters._
        val landingData = spark.createDataFrame(landingRawSourceData.asJava, landingRawStruct)

        // when
        val actual = FileIngestionFrameworkTransformations.transformSegmint(landingData)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs Seq("record_type", "record_value", "feed_date", "file_name", "cxi_id")
        }
        val actualSegmintRawData = actual.collect()
        withClue("Segmint raw data do not match") {
            val expected = List(
                ("age_category",
                """"2021-02"|"FOOD AND DINING"|"JAPANESE NOODLE RESTAURANTS"|""|"AGE: 0-20"|1|1|41.59|41.59""",
                "2022-02-06", "202102_cxi_age_category.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1a"),
                ("age_merch",
                """"2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
                "2022-02-06", "202102_cxi_age_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1b"),
                ("zip_merch",
                """"2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
                "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1c"),
                ("zip_merch",
                """"2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
                "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1d"),
                ("zip_merch",
                """"2021-02"|"CARL'S JR."|"FOOD AND DINING"|"FAST FOOD HAMBURGER RESTAURANTS"|""|""|93|106|3068.61|28.95""",
                "2022-02-06", "202102_cxi_zip_merch.csv.gz", "5635dc0e-637f-4765-85ea-974560384d1e")
            ).toDF("record_type", "record_value", "feed_date", "file_name", "cxi_id").collect()
            actualSegmintRawData.length should equal(expected.length)
            actualSegmintRawData should contain theSameElementsAs expected
        }
    }
}

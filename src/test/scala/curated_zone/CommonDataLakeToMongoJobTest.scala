package com.cxi.cdp.data_processing
package curated_zone

import support.utils.TransformUtils.{CastDataType, DestCol, SourceCol}
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class CommonDataLakeToMongoJobTest extends BaseSparkBatchJobTest {

    test("test process source dataframe without transformations") {
        // given
        import spark.implicits._
        val tempTable = "temp_signal_universe"
        val sourceDf = Seq(
            ("food_and_drink_preference", "burger_affinity", "boolean", true),
            ("campaign", "reachability_email", "boolean", false),
            ("channel_preferences", "delivery_preferred", "float", true)
        ).toDF("domain_name", "signal_name", "es_type", "is_active")
        sourceDf.createOrReplaceTempView(tempTable)

        // when
        val actual = CommonDataLakeToMongoJob.process(spark, tempTable, None, includeAllSourceColumns = true)

        // then
        val actualFieldsReturned = actual.schema.fields.map(_.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldBe Array("domain_name", "signal_name", "es_type", "is_active")
        }
        val actualData = actual.collect()
        withClue("Transform data do not match") {
            val expected = sourceDf.collect()
            actualData.length should equal(expected.length)
            actualData should contain theSameElementsAs expected
        }
    }

    test("test take only mapped columns and transform") {
        // given
        import spark.implicits._
        val tempTable = "temp_signal_universe"
        val sourceDf = Seq(
            ("food_and_drink_preference", "burger_affinity", "boolean", true, "1.0"),
            ("campaign", "reachability_email", "boolean", false, "3.0"),
            ("channel_preferences", "delivery_preferred", "float", true, "2.0")
        ).toDF("domain_name", "signal_name", "es_type", "is_active", "number")
        sourceDf.createOrReplaceTempView(tempTable)

        val columnsMappingOptions = Option(
            Seq(
                Map(SourceCol -> "domain_name", DestCol -> "signal_domain_name"),
                Map(SourceCol -> "signal_name", DestCol -> "signal_name"),
                Map(SourceCol -> "number", DestCol -> "number_long", CastDataType -> "long")
            )
        )

        // when
        val actual =
            CommonDataLakeToMongoJob.process(spark, tempTable, columnsMappingOptions, includeAllSourceColumns = false)

        // then
        val actualFieldsReturned = actual.schema.fields.map(_.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldBe Array("signal_domain_name", "signal_name", "number_long")
        }
        val actualData = actual.collect()
        withClue("Transform data do not match") {
            val expected = Seq(
                ("food_and_drink_preference", "burger_affinity", 1),
                ("campaign", "reachability_email", 3),
                ("channel_preferences", "delivery_preferred", 2)
            ).toDF("signal_domain_name", "signal_name", "number_long").collect()
            actualData.length should equal(expected.length)
            actualData should contain theSameElementsAs expected
        }
    }

}

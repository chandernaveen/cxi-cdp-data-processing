package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import support.BaseSparkBatchJobTest

import model.qu_beyond._
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class FileIngestionFrameworkQuBeyondTransformationTest extends BaseSparkBatchJobTest {

    test("test QuBeyond transformation with Checks data") {
        // given
        val check1 = Check(check_id = "613a24bde021c2301d5f4e24", location_id = Some(317))
        val check2 = check1.copy(check_id = "987a48b26a6c897a6ae08efd")
        val landingQuBeyondCheckData = QuBeyondLandingZoneModel(
            data = Data(check = Array(check1, check2)),
            data_delta = DataDelta(date = "09272021", time = "0257"),
            feed_date = "2021-09-03",
            cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
            file_name = "check.json"
        )

        // when
        val actual = FileIngestionFrameworkTransformations.transformQuBeyond(
            spark.createDataFrame(List(landingQuBeyondCheckData))
        )

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "record_type",
                "record_value",
                "feed_date",
                "file_name",
                "cxi_id",
                "req_customer_id",
                "req_location_id",
                "req_data_type",
                "req_sub_data_type",
                "data_delta",
                "req_start_date",
                "req_end_date"
            )
        }
        import spark.implicits._
        val actualQuBeyondRawData = actual.as[QuBeyondRawZoneModel].collectAsList()
        withClue(s"""Check data do not match
               | $actualQuBeyondRawData
               |""".stripMargin) {
            actualQuBeyondRawData.size() should equal(landingQuBeyondCheckData.data.check.length)
            val expected = List(
                QuBeyondRawZoneModel(
                    feed_date = landingQuBeyondCheckData.feed_date,
                    cxi_id = landingQuBeyondCheckData.cxi_id,
                    file_name = landingQuBeyondCheckData.file_name,
                    data_delta = DataDelta(date = "09272021", time = "0257"),
                    record_type = "check",
                    record_value = s"""{"check_id":"${check1.check_id}","location_id":${check1.location_id.get}}"""
                ),
                QuBeyondRawZoneModel(
                    feed_date = landingQuBeyondCheckData.feed_date,
                    cxi_id = landingQuBeyondCheckData.cxi_id,
                    file_name = landingQuBeyondCheckData.file_name,
                    data_delta = DataDelta(date = "09272021", time = "0257"),
                    record_type = "check",
                    record_value = s"""{"check_id":"${check2.check_id}","location_id":${check2.location_id.get}}"""
                )
            )

            actualQuBeyondRawData should contain theSameElementsAs expected
        }
    }
}

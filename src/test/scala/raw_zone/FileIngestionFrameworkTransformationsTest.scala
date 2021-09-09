package com.cxi.cdp.data_processing
package raw_zone

import model.{DetailLine, GuestCheck, OracleSimphonyLabLandingModel, OracleSimphonyLabRawModel}
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class FileIngestionFrameworkTransformationsTest extends BaseSparkBatchJobTest {
    before {
        conf.set("hive.exec.dynamic.partition", "true")
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    test("test identity transformation") {
        // given
        val expected = spark.createDataFrame(List((1, 1.0), (2, 2.0), (3, 3.0))).toDF("id", "v")

        // when
        val actual = FileIngestionFrameworkTransformations.identity(expected)

        // then
        assertDataFrameEquals(expected, actual)
    }

    test("test column names to underscores transformation") {
        // given
        val expected = spark.createDataFrame(List((1, 1.0), (2, 2.0), (3, 3.0))).toDF("this is id", "v")

        // when
        val actual = FileIngestionFrameworkTransformations.spaceToUnderScoreInColumnNamesTransformation(expected)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("this_is_id", "v")
        }
    }

    test("test Oracle Simphony transformation with Guest Checks data") {
        // given
        val check1 = GuestCheck(chkNum = Some(42), detailLines = Array(DetailLine(guestCheckLineItemId = Some(7208833))))
        val check2 = check1.copy(chkNum = Some(101))
        val landingOracleSimGuestCheckData = OracleSimphonyLabLandingModel(
            curUTC = "2021-08-11T14:54:57",
            locRef = "P300002",
            feed_date = "2021-09-02",
            cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
            file_name = "getGuestChecks.json",
            guestChecks = Array(check1, check2)
        )

        // when
        val actual = FileIngestionFrameworkTransformations.oracleSim(spark.createDataFrame(List(landingOracleSimGuestCheckData)))

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cur_utc", "loc_ref", "bus_dt", "opn_bus_dt", "latest_bus_dt", "record_type", "record_value", "feed_date", "cxi_id", "file_name")
        }
        import spark.implicits._
        val actualOracleSimRawData = actual.as[OracleSimphonyLabRawModel].collectAsList()
        withClue(
            s"""Guest check data do not match
               | $actualOracleSimRawData
               |""".stripMargin) {
            actualOracleSimRawData.size() should equal(landingOracleSimGuestCheckData.guestChecks.length)
            val expected = List(
                OracleSimphonyLabRawModel(
                    cur_utc = landingOracleSimGuestCheckData.curUTC,
                    loc_ref = landingOracleSimGuestCheckData.locRef,
                    feed_date = landingOracleSimGuestCheckData.feed_date,
                    cxi_id = landingOracleSimGuestCheckData.cxi_id,
                    file_name = landingOracleSimGuestCheckData.file_name,
                    record_type = "guestChecks",
                    record_value = s"""{"chkNum":${check1.chkNum.get},"detailLines":[{"guestCheckLineItemId":${check1.detailLines(0).guestCheckLineItemId.get}}]}"""),
                OracleSimphonyLabRawModel(
                    cur_utc = landingOracleSimGuestCheckData.curUTC,
                    loc_ref = landingOracleSimGuestCheckData.locRef,
                    feed_date = landingOracleSimGuestCheckData.feed_date,
                    cxi_id = landingOracleSimGuestCheckData.cxi_id,
                    file_name = landingOracleSimGuestCheckData.file_name,
                    record_type = "guestChecks",
                    record_value = s"""{"chkNum":${check2.chkNum.get},"detailLines":[{"guestCheckLineItemId":${check2.detailLines(0).guestCheckLineItemId.get}}]}"""))

            actualOracleSimRawData should contain theSameElementsAs expected
        }
    }
}

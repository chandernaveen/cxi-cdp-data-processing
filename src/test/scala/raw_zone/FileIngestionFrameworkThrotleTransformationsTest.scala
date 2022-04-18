package com.cxi.cdp.data_processing
package raw_zone

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class FileIngestionFrameworkThrotleTransformationsTest extends BaseSparkBatchJobTest {

    test("test throtle transformation ad_id ") {
        // given
        val landingThrotleData = spark.createDataFrame(
            List(
                ("123", "null", "aed88141-73ae-410d-a033-faa4ef471b70"),
                ("234", "233223", "c772c5d3-7699-4c71-932e-178f7345501d"),
                ("543", "123", "208c3c24-cdcd-481c-8eba-d145aae2b000")
            ))
            .toDF("throtle_id", "throtle_hhid", "native_maid")

        // when
        val actual = FileIngestionFrameworkTransformations.transformThrotle(landingThrotleData)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("throtle_id", "throtle_hhid", "native_maid")
        }
        import spark.implicits._
        withClue("Actual throtle data frame data do not match") {
            val expected = List(
                ("123", "null", "AED88141-73AE-410D-A033-FAA4EF471B70"),
                ("234", "233223", "C772C5D3-7699-4C71-932E-178F7345501D"),
                ("543", "123", "208C3C24-CDCD-481C-8EBA-D145AAE2B000")
            ).toDF("throtle_id", "throtle_hhid", "native_maid").collect()
            actual.collect() should contain theSameElementsAs expected
        }
    }
}

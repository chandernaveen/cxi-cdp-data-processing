package com.cxi.cdp.data_processing
package raw_zone

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class FileIngestionFrameworkOutlogicTransformationsTest extends BaseSparkBatchJobTest {

    test("test outlogic transformation with advertiser_id collisions between platforms") {
        // given
        val expected = spark.createDataFrame(
            List(
                ("uuid1", "AAID"),
                ("uuid1", "IDFA"),
                ("uuid1", "some new platform")
            ))
            .toDF("advertiser_id", "platform")

        // when
        val actual = FileIngestionFrameworkTransformations.transformOutlogic(expected)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("platform","advertiser_id_AAID", "advertiser_id_IDFA", "advertiser_id_UNKNOWN")
        }
        import spark.implicits._
        withClue("Actual outlogic data frame data do not match") {
            val expected = List(
                ("AAID", "uuid1", null, null),
                ("IDFA", null, "uuid1", null),
                ("some new platform", null, null, "uuid1"),
            ).toDF("platform","advertiser_id_AAID", "advertiser_id_IDFA", "advertiser_id_UNKNOWN").collect()
            actual.collect() should contain theSameElementsAs expected
        }
    }
}

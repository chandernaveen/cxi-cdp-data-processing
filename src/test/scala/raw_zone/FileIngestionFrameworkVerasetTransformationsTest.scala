package com.cxi.cdp.data_processing
package raw_zone

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class FileIngestionFrameworkVerasetTransformationsTest extends BaseSparkBatchJobTest {

    test("test veraset transformation ad_id and IP conversion") {
        // given
        val landingVerasetData = spark
            .createDataFrame(
                List(
                    ("uuid1", "aaid", "172.58.225.114"),
                    ("uuid1", "idfa", "2600:6c56:7e08::"),
                    ("uuid1", "frfr", null)
                )
            )
            .toDF("ad_id", "id_type", "ip_address")

        // when
        val actual = FileIngestionFrameworkTransformations.transformVeraset(landingVerasetData)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "id_type",
                "advertiser_id_AAID",
                "advertiser_id_IDFA",
                "advertiser_id_UNKNOWN",
                "ipv_4",
                "ipv_6"
            )
        }
        import spark.implicits._
        withClue("Actual veraset data frame data do not match") {
            val expected = List(
                ("AAID", "uuid1", null, null, "172.58.225.114", null),
                ("IDFA", null, "uuid1", null, null, "2600:6c56:7e08::"),
                ("FRFR", null, null, "uuid1", null, null)
            ).toDF("id_type", "advertiser_id_AAID", "advertiser_id_IDFA", "advertiser_id_UNKNOWN", "ipv_4", "ipv_6")
                .collect()
            actual.collect() should contain theSameElementsAs expected
        }
    }
}

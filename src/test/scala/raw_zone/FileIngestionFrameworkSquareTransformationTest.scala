package com.cxi.cdp.data_processing
package raw_zone

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.Matchers

class FileIngestionFrameworkSquareTransformationTest extends BaseSparkBatchJobTest with Matchers {

    import model.square._
    import model.square.SquareLandingZoneModel._

    test("test Square transformation with customers data") {
        // given
        val customer1 = Customer(
            address = Address(address_line_1 = "customer_1_address_line_1", country = "USA"),
            note = "customer_1_note"
        )

        val customer2 = Customer(id = "customer_2_id", family_name = "customer_2_family_name")

        val landingData = SquareLandingZoneModel(
            customers = Seq(customer1, customer2),
            feed_date = "2021-09-03",
            cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
            file_name = "check.json"
        )

        val landingDataWithoutCursor = spark.createDataFrame(List(landingData))
        val landingDataWithCursor = landingDataWithoutCursor
            .select(lit("test-cursor").as("cursor"), col("*"))

        // when
        val actual = FileIngestionFrameworkTransformations.transformSquare(landingDataWithoutCursor)
        val actualWithCursor = FileIngestionFrameworkTransformations.transformSquare(landingDataWithCursor)

        // then
        withClue("Data does not match if 'cursor' is present in landing") {
            assertDataFrameDataEquals(actual, actualWithCursor)
        }

        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs Seq(
                "record_type",
                "record_value",
                "feed_date",
                "cxi_id",
                "file_name"
            )
        }

        import spark.implicits._
        val actualRawData = actual.as[SquareRawZoneModel].collectAsList()
        withClue(s"""Customer check data does not match:
               | $actualRawData
               |""".stripMargin) {
            actualRawData.size shouldBe landingData.customers.size

            val expected = List(
                SquareRawZoneModel(
                    record_type = "customers",
                    record_value =
                        """{"address":{"address_line_1":"customer_1_address_line_1","country":"USA"},"note":"customer_1_note"}""",
                    feed_date = "2021-09-03",
                    cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
                    file_name = "check.json"
                ),
                SquareRawZoneModel(
                    record_type = "customers",
                    record_value = """{"family_name":"customer_2_family_name","id":"customer_2_id"}""",
                    feed_date = "2021-09-03",
                    cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
                    file_name = "check.json"
                )
            )

            actualRawData should contain theSameElementsAs expected
        }
    }
}

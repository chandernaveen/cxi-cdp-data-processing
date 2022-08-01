package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import raw_zone.file_ingestion_framework.FileIngestionFrameworkToastTransformationsTest.{
    ToastLandingZoneModel,
    ToastRawZoneModel
}
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class FileIngestionFrameworkToastTransformationsTest extends BaseSparkBatchJobTest {
    import spark.implicits._

    test("test transformToast") {
        // given
        spark.conf.set("spark.sql.caseSensitive", value = true)

        val orderFileName1 =
            "dbfs:/mnt/landing_zone/cxi/pos_toast/dc-test-partner/20220707/orders/restaurant=f987f24a-2ee9-4550-8e76-1fad471c1136/orders_f5b0307a-aaf5-46f6-ae6e-75027611c709_81ec6e4d-12aa-4ea9-9aa8-ae9df53df89d.json"
        val row1 = ToastLandingZoneModel(
            file_name = orderFileName1,
            value = """
                  | {
                  | "modifiedDate": "2017-10-20T19:43:27.520+0000",
                  | "checks": [{"amount": 20}, {"amount": 30}],
                  | "entityType": "Order"
                  | }
                  |""".stripMargin
        )
        val restaurantsFileName =
            "dbfs:/mnt/landing_zone/cxi/pos_toast/dc-test-partner/20220707/restaurants/restaurants_dc0db4ca-54ac-4509-b53b-b9fe30381818_bca9619d-97ff-4e5b-819d-c4b17d90757e.json"
        val row2 = ToastLandingZoneModel(
            file_name = restaurantsFileName,
            value = """
                  | {
                  | "ModifiedDate": "2022-02-24T12:01:22.130+0000",
                  | "restaurantGuid": "guid1"
                  | }
                  |""".stripMargin
        )
        val diningOptionsFileName =
            "dbfs:/mnt/landing_zone/cxi/pos_toast/dc-test-partner/20220707/dining-options/dining-options_4dc0a767-ddc4-4520-979a-40ef20707801_a40226d3-078c-4299-aea7-30065c88e3f8.json"
        val row3 = ToastLandingZoneModel(
            file_name = diningOptionsFileName,
            value = """
                  | {
                  | "diningOption": "some_other dining option",
                  | "ModifiedDate": "2022-02-24T12:01:22.130+0000"
                  | }
                  |""".stripMargin
        )
        val menuFileName =
            "dbfs:/mnt/landing_zone/cxi/pos_toast/cxi-usa-12edc/20220720/menus/menus_64a9f19b-81b2-44ee-957e-0ea22827eb54_64b840b1-d267-4f54-9a8e-9fe680820c95.json"
        val row4 = ToastLandingZoneModel(
            file_name = menuFileName,
            value =
                """ {  "restaurantGuid": "f11f761e-2c7a-45e9-9727-e3a53a5b2be0",  "lastUpdated": "2022-05-31T13:25:44.572+0000",  "restaurantTimeZone": "America/Chicago",  "menus": [    {      "name": "Shakes",      "guid": "63d12c97-3936-4c10-9f29-321701606f79",      "multiLocationId": "5740003381287368",      "masterId": 5740003381287368,      "visibility": [        "POS"      ]    }  ] }"""
        )
        val df = spark.createDataFrame(List(row1, row2, row3, row4))

        // when
        val actual = FileIngestionFrameworkTransformations.transformToast(df)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Array("record_type", "record_value", "location_id", "feed_date", "file_name", "cxi_id")
        }
        val actualRawData = actual.as[ToastRawZoneModel].collectAsList()
        withClue("Toast raw data does not match:\n") {
            val expected = List(
                ToastRawZoneModel(
                    record_type = "orders",
                    file_name = orderFileName1,
                    record_value =
                        """{"checks":[{"amount":20},{"amount":30}],"entityType":"Order","modifiedDate":"2017-10-20T19:43:27.520+0000"}""".stripMargin,
                    location_id = "f987f24a-2ee9-4550-8e76-1fad471c1136"
                ),
                ToastRawZoneModel(
                    record_type = "restaurants",
                    file_name = restaurantsFileName,
                    record_value =
                        """{"ModifiedDate":"2022-02-24T12:01:22.130+0000","restaurantGuid":"guid1"}""".stripMargin
                ),
                ToastRawZoneModel(
                    record_type = "dining-options",
                    file_name = diningOptionsFileName,
                    record_value =
                        """{"ModifiedDate":"2022-02-24T12:01:22.130+0000","diningOption":"some_other dining option"}""".stripMargin
                ),
                ToastRawZoneModel(
                    record_type = "menus",
                    file_name = menuFileName,
                    record_value =
                        """ {  "restaurantGuid": "f11f761e-2c7a-45e9-9727-e3a53a5b2be0",  "lastUpdated": "2022-05-31T13:25:44.572+0000",  "restaurantTimeZone": "America/Chicago",  "menus": [    {      "name": "Shakes",      "guid": "63d12c97-3936-4c10-9f29-321701606f79",      "multiLocationId": "5740003381287368",      "masterId": 5740003381287368,      "visibility": [        "POS"      ]    }  ] }"""
                )
            )

            actualRawData should contain theSameElementsAs expected
        }
        spark.conf.set("spark.sql.caseSensitive", value = false)
    }
}

object FileIngestionFrameworkToastTransformationsTest {

    /** A subset of fields from the production Toast model to be used in tests.
      * The actual model is much more complex.
      */
    case class ToastLandingZoneModel(
        value: String = null,
        feed_date: String = null,
        cxi_id: String = null,
        file_name: String = null
    )

    case class ToastRawZoneModel(
        record_type: String,
        record_value: String,
        feed_date: String = null,
        cxi_id: String = null,
        file_name: String = null,
        location_id: String = null
    )
}

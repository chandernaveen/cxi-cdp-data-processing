package com.cxi.cdp.data_processing
package raw_zone.pos_parbrink.udf

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class ParbrinkUdfsTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("transform Parbrink filepath to record type") {

        // given
        val sourceDf = Seq(
            (
                "id_1",
                "location=123/items_2a9f376b-1935-41b9-93e5-2f12ff136a00_95a59a8b-277b-4efc-86af-fe694045a4d2.json"
            ),
            (
                "id_2",
                "location=123/orders_d111c471-039c-4e96-8590-3a8b087da118_81d715cc-678d-410f-8ae8-358d1e91f5f5.json"
            ),
            (
                "id_3",
                "location=456/tenders_cc3d1191-14a1-4790-87d4-3e1513c4b5d3_d21b48ea-3f05-4a15-a920-d806a5816e03.json"
            ),
            (
                "id_4",
                "location=XYZ/houseAccounts_a20cba01-8646-4090-9830-1c8a91e9e3ea_aa628c94-eba1-4fbd-9ce9-8574c096fc98.json"
            ),
            (
                "id_5",
                "location=123/options_2e663a13-16c2-4e2a-81b8-84a771708bff_ed6d4e5a-cc4b-435c-89f1-59fd4c481456.json"
            )
        ).toDF("cxi_id", "file_name")

        // when
        val actual = sourceDf.withColumn("record_type", ParbrinkUdfs.recordTypeByFilePath(col("file_name")))

        // then
        withClue("failed transform Parbrink filepath to record type") {
            val expected = Seq(
                (
                    "id_1",
                    "location=123/items_2a9f376b-1935-41b9-93e5-2f12ff136a00_95a59a8b-277b-4efc-86af-fe694045a4d2.json",
                    "items"
                ),
                (
                    "id_2",
                    "location=123/orders_d111c471-039c-4e96-8590-3a8b087da118_81d715cc-678d-410f-8ae8-358d1e91f5f5.json",
                    "orders"
                ),
                (
                    "id_3",
                    "location=456/tenders_cc3d1191-14a1-4790-87d4-3e1513c4b5d3_d21b48ea-3f05-4a15-a920-d806a5816e03.json",
                    "tenders"
                ),
                (
                    "id_4",
                    "location=XYZ/houseAccounts_a20cba01-8646-4090-9830-1c8a91e9e3ea_aa628c94-eba1-4fbd-9ce9-8574c096fc98.json",
                    "houseAccounts"
                ),
                (
                    "id_5",
                    "location=123/options_2e663a13-16c2-4e2a-81b8-84a771708bff_ed6d4e5a-cc4b-435c-89f1-59fd4c481456.json",
                    "options"
                )
            ).toDF("cxi_id", "file_name", "record_type")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("transform Parbrink filepath to location") {

        // given
        val sourceDf = Seq(
            (
                "id_1",
                "location=123/items_2a9f376b-1935-41b9-93e5-2f12ff136a00_95a59a8b-277b-4efc-86af-fe694045a4d2.json"
            ),
            (
                "id_2",
                "location=123/orders_d111c471-039c-4e96-8590-3a8b087da118_81d715cc-678d-410f-8ae8-358d1e91f5f5.json"
            ),
            (
                "id_3",
                "location=456/tenders_cc3d1191-14a1-4790-87d4-3e1513c4b5d3_d21b48ea-3f05-4a15-a920-d806a5816e03.json"
            ),
            (
                "id_4",
                "location=XYZ/houseAccounts_a20cba01-8646-4090-9830-1c8a91e9e3ea_aa628c94-eba1-4fbd-9ce9-8574c096fc98.json"
            ),
            (
                "id_5",
                "location=123/options_2e663a13-16c2-4e2a-81b8-84a771708bff_ed6d4e5a-cc4b-435c-89f1-59fd4c481456.json"
            )
        ).toDF("cxi_id", "file_name")

        // when
        val actual = sourceDf.withColumn("location_id", ParbrinkUdfs.locationByFilePath(col("file_name")))

        // then
        withClue("failed transform Parbrink filepath to location") {
            val expected = Seq(
                (
                    "id_1",
                    "location=123/items_2a9f376b-1935-41b9-93e5-2f12ff136a00_95a59a8b-277b-4efc-86af-fe694045a4d2.json",
                    "123"
                ),
                (
                    "id_2",
                    "location=123/orders_d111c471-039c-4e96-8590-3a8b087da118_81d715cc-678d-410f-8ae8-358d1e91f5f5.json",
                    "123"
                ),
                (
                    "id_3",
                    "location=456/tenders_cc3d1191-14a1-4790-87d4-3e1513c4b5d3_d21b48ea-3f05-4a15-a920-d806a5816e03.json",
                    "456"
                ),
                (
                    "id_4",
                    "location=XYZ/houseAccounts_a20cba01-8646-4090-9830-1c8a91e9e3ea_aa628c94-eba1-4fbd-9ce9-8574c096fc98.json",
                    "XYZ"
                ),
                (
                    "id_5",
                    "location=123/options_2e663a13-16c2-4e2a-81b8-84a771708bff_ed6d4e5a-cc4b-435c-89f1-59fd4c481456.json",
                    "123"
                )
            ).toDF("cxi_id", "file_name", "location_id")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}

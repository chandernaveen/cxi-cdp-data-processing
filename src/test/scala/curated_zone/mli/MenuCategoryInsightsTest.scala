package com.cxi.cdp.data_processing
package curated_zone.mli

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class MenuCategoryInsightsTest extends BaseSparkBatchJobTest {

    test("test category read item aggr and join with item univ table ") {
        // given

        import spark.implicits._
        val cxiPartnerId1 = "foo-partner-id-1"
        val cxiPartnerId2 = "bar-partner-id-2"

        val itemInsightsDF = List(
            (cxiPartnerId1, "Burger", "A300003", "Loc 2", "city_1", "state_1", "region_1", "2022-12-10", 100, 10, 1),
            (cxiPartnerId2, "Pizza", "A300003", "Loc 3", "city_3", "state_3", "region_1", "2022-12-12", 100, 10, 1),
            (cxiPartnerId2, "Pizza", "A300003", "Loc 3", "city_3", "state_3", "region_1", "2022-12-13", 100, 10, 1)
        ).toDF(
            "cxi_partner_id",
            "pos_item_nm",
            "location_id",
            "location_nm",
            "city",
            "state_code",
            "region",
            "ord_date",
            "item_total",
            "item_quantity",
            "transaction_quantity"
        )
        val itemInsightsTable: String = "itemInsightsTable"
        itemInsightsDF.createOrReplaceGlobalTempView(itemInsightsTable)
        val orderDates: Set[String] = Set("2022-12-10", "2022-12-12")
        val itemUniverseDF = List(
            (cxiPartnerId1, "Fast-Food", "Burger", null, null, "N", "N"),
            (cxiPartnerId1, "Unhealthy", "Burger", null, null, "N", "N"),
            (cxiPartnerId2, "Fast-Food", "Pizza", "FF", null, "Y", "N")
        )
            .toDF(
                "cxi_partner_id",
                "cxi_item_category",
                "pos_item_nm",
                "item_category_modified",
                "item_nm_modified",
                "is_category_modified",
                "is_item_modified"
            )

        val itemUniverseTable: String = "itemUniverseTable"
        itemUniverseDF.createOrReplaceGlobalTempView(itemUniverseTable)
        // when
        val actual = MenuCategoryInsights.readItemInsightsUniv(
            orderDates,
            s"global_temp.$itemInsightsTable",
            s"global_temp.$itemUniverseTable"
        )(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq(
                    "ord_date",
                    "cxi_partner_id",
                    "region",
                    "state_code",
                    "city",
                    "location_id",
                    "location_nm",
                    "transaction_quantity",
                    "item_quantity",
                    "item_total",
                    "item_category",
                    "item_nm"
                )
        }
        val actualPartnerMarketInsightsData = actual.collect()
        withClue("Menu Item category insights Read data do not match") {
            val expected = List(
                (
                    "2022-12-10",
                    cxiPartnerId1,
                    "region_1",
                    "state_1",
                    "city_1",
                    "A300003",
                    "Loc 2",
                    1,
                    10,
                    100,
                    "Unhealthy",
                    "Burger"
                ),
                (
                    "2022-12-10",
                    cxiPartnerId1,
                    "region_1",
                    "state_1",
                    "city_1",
                    "A300003",
                    "Loc 2",
                    1,
                    10,
                    100,
                    "Fast-Food",
                    "Burger"
                ),
                (
                    "2022-12-12",
                    cxiPartnerId2,
                    "region_1",
                    "state_3",
                    "city_3",
                    "A300003",
                    "Loc 3",
                    1,
                    10,
                    100,
                    "FF",
                    "Pizza"
                )
            ).toDF(
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "transaction_quantity",
                "item_quantity",
                "item_total",
                "item_category",
                "item_nm"
            ).collect()
            actualPartnerMarketInsightsData.length should equal(expected.length)
            actualPartnerMarketInsightsData should contain theSameElementsAs expected
        }
    }

    test("test category aggregation insights computation") {
        // given
        import spark.implicits._

        val cxiPartnerId1 = "foo-partner-id-1"
        val cxiPartnerId2 = "bar-partner-id-2"
        val itemInsightsDataDf = List(
            (
                "2022-12-10",
                cxiPartnerId1,
                "region_1",
                "state_1",
                "city_1",
                "A300003",
                "Loc 2",
                1,
                10,
                100,
                "Fast-Food",
                "Burger"
            ),
            (
                "2022-12-10",
                cxiPartnerId1,
                "region_1",
                "state_1",
                "city_1",
                "A300003",
                "Loc 2",
                1,
                10,
                100,
                "Unhealthy",
                "Burger"
            ),
            (
                "2022-12-12",
                cxiPartnerId2,
                "region_1",
                "state_3",
                "city_3",
                "A300003",
                "Loc 3",
                1,
                10,
                100,
                "FF",
                "Pizza"
            )
        ).toDF(
            "ord_date",
            "cxi_partner_id",
            "region",
            "state_code",
            "city",
            "location_id",
            "location_nm",
            "transaction_quantity",
            "item_quantity",
            "item_total",
            "item_category",
            "item_nm"
        )

        // when
        val actual = MenuCategoryInsights.computeCategoryInsights(itemInsightsDataDf)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq(
                    "ord_date",
                    "cxi_partner_id",
                    "region",
                    "state_code",
                    "city",
                    "location_id",
                    "location_nm",
                    "transaction_quantity",
                    "item_quantity",
                    "item_category",
                    "item_total"
                )
        }
        val actualPartnerMarketInsightsData = actual.collect()
        withClue("Menu Item category insights aggr data do not match") {
            val expected = List(
                (
                    "2022-12-10",
                    cxiPartnerId1,
                    "region_1",
                    "state_1",
                    "city_1",
                    "A300003",
                    "Loc 2",
                    1,
                    10,
                    "Fast-Food",
                    100
                ),
                (
                    "2022-12-10",
                    cxiPartnerId1,
                    "region_1",
                    "state_1",
                    "city_1",
                    "A300003",
                    "Loc 2",
                    1,
                    10,
                    "Unhealthy",
                    100
                ),
                ("2022-12-12", cxiPartnerId2, "region_1", "state_3", "city_3", "A300003", "Loc 3", 1, 10, "FF", 100)
            ).toDF(
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "transaction_amount",
                "item_quantity",
                "item_category",
                "item_total"
            ).collect()
            actualPartnerMarketInsightsData.length should equal(expected.length)
            actualPartnerMarketInsightsData should contain theSameElementsAs expected
        }
    }

    test("test getOrderDatesToProcess for empty change data") {
        import spark.implicits._
        val df = List.empty[java.sql.Date].toDF("ord_date")
        MenuCategoryInsights.getItemOrderDatesToProcess(df) shouldBe Set.empty
    }

    test("test getOrderDatesToProcess for non-empty change data with nulls") {
        import spark.implicits._
        val df = List(sqlDate(2021, 10, 1), null, sqlDate(2021, 10, 2)).toDF("ord_date")
        MenuCategoryInsights.getItemOrderDatesToProcess(df) shouldBe Set("2021-10-01", "2021-10-02")
    }

    private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

}

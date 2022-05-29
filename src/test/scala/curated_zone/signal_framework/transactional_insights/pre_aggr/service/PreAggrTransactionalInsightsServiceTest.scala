package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import com.cxi.cdp.data_processing.curated_zone.audience.model.Customer360
import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights.OrderMetric
import com.cxi.cdp.data_processing.curated_zone.signal_framework.transactional_insights.pre_aggr.model._
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity.CxiIdentityIds
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest

import org.scalatest.Matchers

import java.sql.Date

class PreAggrTransactionalInsightsServiceTest extends BaseSparkBatchJobTest with Matchers {

    import MetricsServiceHelper._
    import PreAggrTransactionalInsightsRecord.AllLocationsAlias
    import PreAggrTransactionalInsightsService._
    import PreAggrTransactionalInsightsServiceTest._

    test("getCustomer360IdWithMetrics") {
        // given
        import spark.implicits._

        val orderSummaryWithMetrics = List(
            // single partner / location / customer 360 ID
            (
                "1",
                Date.valueOf("2022-03-01"),
                "partner-1",
                "location-1",
                Seq(CxiIdentityId("phone", "111")),
                Some(1L),
                Some(1234L)
            ),

            // multiple locations, partners, customer 360 ID-to-cxiIdentityId mappings
            // customer-1
            (
                "2",
                Date.valueOf("2022-03-02"),
                "partner-2",
                "location-1",
                Seq(CxiIdentityId("phone", "111")),
                Some(13L),
                Some(7800L)
            ),
            (
                "3",
                Date.valueOf("2022-03-02"),
                "partner-2",
                "location-2",
                Seq(CxiIdentityId("phone", "111")),
                Some(2L),
                Some(300L)
            ),
            (
                "4",
                Date.valueOf("2022-03-02"),
                "partner-2",
                "location-2",
                Seq(CxiIdentityId("phone", "111")),
                Some(3L),
                Some(500L)
            ),
            // customer-2
            (
                "5",
                Date.valueOf("2022-03-02"),
                "partner-1",
                "location-1",
                Seq(CxiIdentityId("phone", "222"), CxiIdentityId("phone", "333")),
                Some(2L),
                Some(2345L)
            ),
            (
                "6",
                Date.valueOf("2022-03-02"),
                "partner-1",
                "location-2",
                Seq(CxiIdentityId("phone", "222")),
                Some(3L),
                Some(3456L)
            ),
            (
                "7",
                Date.valueOf("2022-03-02"),
                "partner-2",
                "location-1",
                Seq(CxiIdentityId("phone", "333")),
                Some(2L),
                Some(5000L)
            ),

            // no corresponding customer 360 ID
            (
                "8",
                Date.valueOf("2022-03-03"),
                "partner-1",
                "location-1",
                Seq(CxiIdentityId("phone", "555")),
                Some(4L),
                Some(4567L)
            )
        ).toDF(
            "ord_id",
            "ord_date",
            "cxi_partner_id",
            "location_id",
            CxiIdentityIds,
            metricColumnName(OrderMetric.signalDomainName, OrderMetric.TotalOrders.signalName),
            metricColumnName(OrderMetric.signalDomainName, OrderMetric.TotalAmount.signalName)
        )

        val customer360IdToQualifiedIdentity = List(
            ("customer-1", "phone:111"),
            ("customer-2", "phone:222"),
            ("customer-2", "phone:333")
        ).toDF("customer_360_id", "qualified_identity")

        // when
        val actual =
            getCustomer360IdWithMetrics(orderSummaryWithMetrics, customer360IdToQualifiedIdentity, Seq(OrderMetric))

        // then
        val expected = List(
            ("customer-1", "partner-1", Date.valueOf("2022-03-01"), "location-1", 1L, 1234L),
            ("customer-1", "partner-1", Date.valueOf("2022-03-01"), AllLocationsAlias, 1L, 1234L),
            ("customer-1", "partner-2", Date.valueOf("2022-03-02"), "location-1", 13L, 7800L),
            ("customer-1", "partner-2", Date.valueOf("2022-03-02"), "location-2", 5L, 800L),
            ("customer-1", "partner-2", Date.valueOf("2022-03-02"), AllLocationsAlias, 18L, 8600L),
            ("customer-2", "partner-1", Date.valueOf("2022-03-02"), "location-1", 2L, 2345L),
            ("customer-2", "partner-1", Date.valueOf("2022-03-02"), "location-2", 3L, 3456L),
            ("customer-2", "partner-1", Date.valueOf("2022-03-02"), AllLocationsAlias, 5L, 5801L),
            ("customer-2", "partner-2", Date.valueOf("2022-03-02"), "location-1", 2L, 5000L),
            ("customer-2", "partner-2", Date.valueOf("2022-03-02"), AllLocationsAlias, 2L, 5000L)
        ).toDF(
            "customer_360_id",
            "cxi_partner_id",
            "ord_date",
            "location_id",
            "order_metrics__SEP__total_orders",
            "order_metrics__SEP__total_amount"
        )

        actual.collect should contain theSameElementsAs expected.collect
    }

    test("getCustomer360IdToQualifiedIdentity") {
        // given
        import spark.implicits._

        val dummyDate = java.sql.Date.valueOf("2022-03-20")

        def createCustomer360(customer_360_id: String, identities: Map[String, Seq[String]]) =
            Customer360(customer_360_id, identities, dummyDate, dummyDate, true)

        val input = List(
            createCustomer360(customer_360_id = "customer-1", identities = Map.empty),
            createCustomer360(customer_360_id = "customer-2", identities = Map("phone" -> Seq("111"))),
            createCustomer360(customer_360_id = "customer-3", identities = Map("phone" -> Seq("222", "333"))),
            createCustomer360(
                customer_360_id = "customer-4",
                identities = Map(
                    "phone" -> Seq("444", "555"),
                    "email" -> Seq("first@example.com", "second@example.com", "third@example.com")
                )
            )
        )

        val expected = List(
            ("customer-2", "phone:111"),
            ("customer-3", "phone:222"),
            ("customer-3", "phone:333"),
            ("customer-4", "phone:444"),
            ("customer-4", "phone:555"),
            ("customer-4", "email:first@example.com"),
            ("customer-4", "email:second@example.com"),
            ("customer-4", "email:third@example.com")
        ).toDF("customer_360_id", "qualified_identity")

        // when
        val actual = getCustomer360IdToQualifiedIdentity(input.toDS.toDF)

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("transformToFinalRecord") {
        // given
        import spark.implicits._

        val input = List(
            (Date.valueOf("2022-03-01"), "customer-1", "partner-1", "location-1", Some(3L), Some(1234L)),
            (Date.valueOf("2022-03-02"), "customer-2", "partner-2", "location-2", Some(10L), None),
            (Date.valueOf("2022-03-03"), "customer-3", "partner-3", "location-3", Some(0L), Some(987L)),
            (Date.valueOf("2022-03-04"), "customer-4", "partner-4", "location-4", None, None)
        ).toDF(
            "ord_date",
            "customer_360_id",
            "cxi_partner_id",
            "location_id",
            metricColumnName(OrderMetric.signalDomainName, OrderMetric.TotalOrders.signalName),
            metricColumnName(OrderMetric.signalDomainName, OrderMetric.TotalAmount.signalName)
        )

        val expected = List(
            PreAggrTransactionalInsightsRecord(
                ord_date = Date.valueOf("2022-03-01"),
                customer_360_id = "customer-1",
                cxi_partner_id = "partner-1",
                location_id = "location-1",
                signal_domain = OrderMetric.signalDomainName,
                signal_name = OrderMetric.TotalOrders.signalName,
                signal_value = 3L
            ),
            PreAggrTransactionalInsightsRecord(
                ord_date = Date.valueOf("2022-03-01"),
                customer_360_id = "customer-1",
                cxi_partner_id = "partner-1",
                location_id = "location-1",
                signal_domain = OrderMetric.signalDomainName,
                signal_name = OrderMetric.TotalAmount.signalName,
                signal_value = 1234L
            ),
            PreAggrTransactionalInsightsRecord(
                ord_date = Date.valueOf("2022-03-02"),
                customer_360_id = "customer-2",
                cxi_partner_id = "partner-2",
                location_id = "location-2",
                signal_domain = OrderMetric.signalDomainName,
                signal_name = OrderMetric.TotalOrders.signalName,
                signal_value = 10L
            ),
            PreAggrTransactionalInsightsRecord(
                ord_date = Date.valueOf("2022-03-03"),
                customer_360_id = "customer-3",
                cxi_partner_id = "partner-3",
                location_id = "location-3",
                signal_domain = OrderMetric.signalDomainName,
                signal_name = OrderMetric.TotalAmount.signalName,
                signal_value = 987L
            )
        )

        // when
        val actual = transformToFinalRecord(input, signalDomains = Seq(OrderMetric))(spark).collect

        // then
        actual should contain theSameElementsAs expected
    }

}

object PreAggrTransactionalInsightsServiceTest {
    case class CxiIdentityId(identity_type: String, cxi_identity_id: String)
}

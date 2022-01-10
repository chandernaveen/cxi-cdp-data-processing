package com.cxi.cdp.data_processing
package curated_zone.identity

import com.cxi.cdp.data_processing.curated_zone.identity.model.{IdentityId, IdentityRelationship, RelatedIdentities}
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest
import org.scalatest.Matchers

class UpdateIdentityRelationshipsJobTest extends BaseSparkBatchJobTest with Matchers {

    import UpdateIdentityRelationshipsJobTest._

    test("extractRelatedEntities") {
        import spark.implicits._

        val input = Seq(
            OrderSummary(null, sqlDate(2021, 10, 11)),
            OrderSummary(Seq(), sqlDate(2021, 10, 12)),
            OrderSummary(Seq(IdentityId("customer_type_1", "cxi_identity_id_1")), sqlDate(2021, 10, 13)),
            OrderSummary(
                Seq(
                    IdentityId("customer_type_2", "cxi_identity_id_2"),
                    IdentityId("customer_type_3", "cxi_identity_id_3")),
                sqlDate(2021, 10, 14)),
            OrderSummary(
                Seq(
                    IdentityId("customer_type_4", "cxi_identity_id_4"),
                    IdentityId("customer_type_5", "cxi_identity_id_5"),
                    IdentityId("customer_type_6", "cxi_identity_id_6")),
                sqlDate(2021, 10, 15))
        )

        val expected = Seq(
            RelatedIdentities(
                Seq(
                    IdentityId("customer_type_2", "cxi_identity_id_2"),
                    IdentityId("customer_type_3", "cxi_identity_id_3")),
                sqlDate(2021, 10, 14)),
            RelatedIdentities(
                Seq(
                    IdentityId("customer_type_4", "cxi_identity_id_4"),
                    IdentityId("customer_type_5", "cxi_identity_id_5"),
                    IdentityId("customer_type_6", "cxi_identity_id_6")),
                sqlDate(2021, 10, 15))
        )

        val actual = UpdateIdentityRelationshipsJob.extractRelatedEntities(input.toDF)(spark).collect

        actual should contain theSameElementsAs expected
    }

    test("mergeIdentityRelationships") {
        val first = IdentityRelationship(
            source = "source_id_1",
            source_type = "source_type_1",
            target = "target_id_1",
            target_type = "target_type_1",
            relationship = "pos_related",
            frequency = 3,
            created_date = sqlDate(2021, 10, 10),
            last_seen_date = sqlDate(2021, 10, 15),
            active_flag = true
        )

        val second = first.copy(
            frequency = 2,
            created_date = sqlDate(2021, 10, 8),
            last_seen_date = sqlDate(2021, 10, 13)
        )

        val expected = first.copy(
            frequency = 5,
            created_date = sqlDate(2021, 10, 8),
            last_seen_date = sqlDate(2021, 10, 15)
        )

        UpdateIdentityRelationshipsJob.mergeIdentityRelationships(first, second) shouldBe expected
    }

    private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

}

object UpdateIdentityRelationshipsJobTest {

    case class OrderSummary(
                               cxi_identity_id_array: Seq[IdentityId],
                               ord_date: java.sql.Date
                           )

}

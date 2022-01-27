package com.cxi.cdp.data_processing
package refined_zone.hub.identity

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model._
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest
import org.scalatest.Matchers

class ExtractIdentityRelationshipsJobTest extends BaseSparkBatchJobTest with Matchers {

    import ExtractIdentityRelationshipsJobTest._

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

        val actual = ExtractIdentityRelationshipsJob.extractRelatedEntities(input.toDF)(spark).collect

        actual should contain theSameElementsAs expected
    }

    test("mergeIdentityRelationships") {
        val first = IdentityRelationship(
            source = "source_id_1",
            source_type = "source_type_1",
            target = "target_id_1",
            target_type = "target_type_1",
            relationship = ExtractIdentityRelationshipsJob.RelationshipType,
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

        ExtractIdentityRelationshipsJob.mergeIdentityRelationships(first, second) shouldBe expected
    }

    test("createIdentityRelationships") {
        import spark.implicits._

        val relatedIdentities = Seq(
            RelatedIdentities(
                Seq(IdentityId("email", "A_cxi_identity_id_0")), // will not result in any relationships
                sqlDate(2021, 10, 10)),
            RelatedIdentities(
                Seq(
                    IdentityId("email", "A_cxi_identity_id_1"),
                    IdentityId("phone", "B_cxi_identity_id_2")),
                sqlDate(2021, 10, 14)),
            RelatedIdentities(
                Seq(
                    IdentityId("email", "A_cxi_identity_id_1"),
                    IdentityId("phone", "B_cxi_identity_id_2"),
                    IdentityId("email", "C_cxi_identity_id_3")),
                sqlDate(2021, 10, 15))
        )

        val actualIdentityRelationships = ExtractIdentityRelationshipsJob
            .createIdentityRelationships(relatedIdentities.toDS)(spark)
            .collect

        val expectedIdentityRelationships = Seq(
            IdentityRelationship(
                source = "A_cxi_identity_id_1",
                source_type = "email",
                target = "B_cxi_identity_id_2",
                target_type = "phone",
                relationship = ExtractIdentityRelationshipsJob.RelationshipType,
                frequency = 2,
                created_date = sqlDate(2021, 10, 14),
                last_seen_date = sqlDate(2021, 10, 15),
                active_flag = true
            ),
            IdentityRelationship(
                source = "A_cxi_identity_id_1",
                source_type = "email",
                target = "C_cxi_identity_id_3",
                target_type = "email",
                relationship = ExtractIdentityRelationshipsJob.RelationshipType,
                frequency = 1,
                created_date = sqlDate(2021, 10, 15),
                last_seen_date = sqlDate(2021, 10, 15),
                active_flag = true
            ),
            IdentityRelationship(
                source = "B_cxi_identity_id_2",
                source_type = "phone",
                target = "C_cxi_identity_id_3",
                target_type = "email",
                relationship = ExtractIdentityRelationshipsJob.RelationshipType,
                frequency = 1,
                created_date = sqlDate(2021, 10, 15),
                last_seen_date = sqlDate(2021, 10, 15),
                active_flag = true
            )
        )

        actualIdentityRelationships should contain theSameElementsAs expectedIdentityRelationships
    }

    private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

}

object ExtractIdentityRelationshipsJobTest {

    case class OrderSummary(
                               cxi_identity_ids: Seq[IdentityId],
                               ord_date: java.sql.Date
                           )

}

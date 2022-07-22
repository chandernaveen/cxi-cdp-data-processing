package com.cxi.cdp.data_processing
package curated_zone.geo_identity

import curated_zone.audience.model.Customer360
import curated_zone.geo_identity.model.{GeoIdentityRow, GeoLocationToPosCustomer360Row}
import refined_zone.hub.geo_identity.model.GeoLocationToOrderRow
import refined_zone.hub.identity.model.{IdentityId, IdentityRelationship}
import support.utils.DateTimeTestUtils._
import support.BaseSparkBatchJobTest

import org.scalatest.{BeforeAndAfterEach, Matchers}

import java.time.LocalDate

class GeoIdentityRelationshipJobTest extends BaseSparkBatchJobTest with Matchers with BeforeAndAfterEach {

    import GeoIdentityRelationshipJob._
    import GeoIdentityRelationshipJobTest._

    test("getOrderDateStart") {
        // May has 31 days
        getOrderDateStart(LocalDate.parse("2022-06-10"), 31) shouldBe LocalDate.parse("2022-05-11")
    }

    test("joinGeoLocationWithOrder") {
        import spark.implicits._

        // given

        val posCustomer360 = Seq(
            Customer360(
                customer_360_id = "customer_1",
                identities = Map("phone" -> Seq("111", "222"), "email" -> Seq("first@example.com")),
                create_date = sqlDate("2022-06-10"),
                update_date = sqlDate("2022-06-14"),
                active_flag = true
            ),
            Customer360(
                customer_360_id = "customer_2",
                identities = Map("email" -> Seq("second@example.com")),
                create_date = sqlDate("2022-06-02"),
                update_date = sqlDate("2022-06-05"),
                active_flag = true
            ),
            // does not have a match
            Customer360(
                customer_360_id = "customer_3",
                identities = Map("email" -> Seq("third@example.com")),
                create_date = sqlDate("2022-06-01"),
                update_date = sqlDate("2022-06-01"),
                active_flag = true
            )
        ).toDF

        val geoLocationToOrder = Seq(
            // matches customer_1
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "222")),
                maid = "maid_1",
                maid_type = "maid_type_1",
                device_score = Some(0.5)
            ),
            // matches customer_1
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_1",
                ord_date = sqlDate("2022-06-10"),
                cxi_identity_ids = Seq(IdentityId("phone", "222")),
                maid = "maid_2",
                maid_type = "maid_type_2",
                device_score = Some(0.5)
            ),
            // matches customer_2
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_1",
                location_id = "location_1",
                ord_id = "order_2",
                ord_date = sqlDate("2022-06-05"),
                cxi_identity_ids = Seq(IdentityId("email", "second@example.com")),
                maid = "maid_1",
                maid_type = "maid_type_1",
                device_score = Some(1.0)
            ),
            // matches customer_1
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_2",
                location_id = "location_2",
                ord_id = "order_3",
                ord_date = sqlDate("2022-06-14"),
                cxi_identity_ids = Seq(IdentityId("email", "first@example.com")),
                maid = "maid_2",
                maid_type = "maid_type_2",
                device_score = Some(0.2)
            ),
            // does not have a match
            GeoLocationToOrderRow(
                cxi_partner_id = "partner_2",
                location_id = "location_2",
                ord_id = "order_4",
                ord_date = sqlDate("2022-06-14"),
                cxi_identity_ids = Seq(IdentityId("email", "fourth@example.com")),
                maid = "maid_2",
                maid_type = "maid_type_2",
                device_score = Some(1.0)
            )
        ).toDF

        // when

        val actual = joinGeoLocationWithCustomer(geoLocationToOrder, posCustomer360)(spark)

        // then

        val expected = Seq(
            IntermediateGeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_1",
                frequency_linked = Some(1),
                profile_maid_link_score = Some(0.5)
            ),
            IntermediateGeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_1",
                frequency_linked = Some(2),
                profile_maid_link_score = Some(0.7)
            ),
            IntermediateGeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_2",
                frequency_linked = Some(1),
                profile_maid_link_score = Some(1.0)
            )
        ).toDF

        // not using `assertDataFrameNoOrderEquals` because of different column nullability inferred by Spark
        assertDataFrameDataEquals(expected, actual)
        actual.schema.fieldNames shouldBe expected.schema.fieldNames
    }

    test("calculateConfidenceScore") {
        import spark.implicits._

        // given

        val minFrequencyLinked = 4

        val geoLocationToCustomer = Seq(
            IntermediateGeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_1",
                frequency_linked = Some(4),
                profile_maid_link_score = Some(0.5)
            ),
            IntermediateGeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_1",
                frequency_linked = Some(5),
                profile_maid_link_score = Some(0.7)
            ),
            // frequency_linked is lower than minFrequencyLinked, so confidence score is 0.0
            IntermediateGeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_2",
                frequency_linked = Some(1),
                profile_maid_link_score = Some(1.0)
            )
        ).toDF

        // when

        val actual = calculateConfidenceScore(geoLocationToCustomer, minFrequencyLinked)(spark)

        // then
        val expectedConfidenceScoreForFirstRecord = (4.0 / 1.2) * (0.5 / 1.2) * Math.pow(0.5 / 1.5, 0.1)
        val expectedConfidenceScoreForSecondRecord = (5.0 / 1.2) * (0.7 / 1.2) * Math.pow(0.7 / 0.7, 0.1)

        val expected = Seq(
            GeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_1",
                frequency_linked = Some(4),
                profile_maid_link_score = Some(0.5),
                total_profiles_per_maid = Some(2L),
                total_score_per_maid = Some(1.5),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(1.2),
                confidence_score = Some(expectedConfidenceScoreForFirstRecord)
            ),
            GeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_1",
                frequency_linked = Some(5),
                profile_maid_link_score = Some(0.7),
                total_profiles_per_maid = Some(1L),
                total_score_per_maid = Some(0.7),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(1.2),
                confidence_score = Some(expectedConfidenceScoreForSecondRecord)
            ),
            GeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_2",
                frequency_linked = Some(1),
                profile_maid_link_score = Some(1.0),
                total_profiles_per_maid = Some(2L),
                total_score_per_maid = Some(1.5),
                total_maids_per_profile = Some(1L),
                total_score_per_profile = Some(1.0),
                confidence_score = Some(0.0)
            )
        ).toDF

        // not using `assertDataFrameNoOrderEquals` because of different column nullability inferred by Spark
        assertDataFrameDataEquals(expected, actual)
        actual.schema.fieldNames shouldBe expected.schema.fieldNames
    }

    test("filterByConfidenceScoreCutoff") {
        import spark.implicits._

        // given
        val geoLocationToCustomer = Seq(
            GeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_1",
                frequency_linked = Some(3),
                profile_maid_link_score = Some(0.5),
                total_profiles_per_maid = Some(2L),
                total_score_per_maid = Some(1.5),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(1.2),
                confidence_score = Some(0.3)
            ),
            GeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_2",
                frequency_linked = Some(5),
                profile_maid_link_score = Some(2.0),
                total_profiles_per_maid = Some(1L),
                total_score_per_maid = Some(5.0),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(10.0),
                confidence_score = Some(0.8)
            )
        ).toDF

        // when
        val actual = filterByConfidenceScoreCutoff(geoLocationToCustomer, confidenceScoreCutoff = 0.5)(spark)

        // then
        val expected = Seq(
            GeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_2",
                frequency_linked = Some(5),
                profile_maid_link_score = Some(2.0),
                total_profiles_per_maid = Some(1L),
                total_score_per_maid = Some(5.0),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(10.0),
                confidence_score = Some(0.8)
            )
        ).toDF

        assertDataFrameNoOrderEquals(expected, actual)
    }

    test("extractGeoIdentity") {
        import spark.implicits._

        // given
        val geoLocationToCustomer = Seq(
            GeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_1",
                frequency_linked = Some(3),
                profile_maid_link_score = Some(0.5),
                total_profiles_per_maid = Some(2L),
                total_score_per_maid = Some(1.5),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(1.2),
                confidence_score = Some(0.3)
            ),
            GeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_2",
                frequency_linked = Some(5),
                profile_maid_link_score = Some(2.0),
                total_profiles_per_maid = Some(1L),
                total_score_per_maid = Some(5.0),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(10.0),
                confidence_score = Some(0.8)
            )
        ).toDF

        // when
        val actual = extractGeoIdentity(geoLocationToCustomer)(spark)

        // then
        val expected = Seq(
            GeoIdentityRow(
                cxi_identity_id = "maid_1",
                `type` = "maid_type_1",
                weight = null,
                metadata = null
            ),
            GeoIdentityRow(
                cxi_identity_id = "maid_2",
                `type` = "maid_type_2",
                weight = null,
                metadata = null
            )
        )

        actual.as[GeoIdentityRow].collect() should contain theSameElementsAs expected
    }

    test("createGeoIdentityRelationship") {
        import spark.implicits._

        // given
        val currentDate = LocalDate.parse("2022-06-10")
        val currentDateSql = java.sql.Date.valueOf(currentDate)

        val geoLocationToCustomer = Seq(
            GeoLocationToPosCustomer360Row(
                maid = "maid_1",
                maid_type = "maid_type_1",
                customer_360_id = "customer_1",
                frequency_linked = Some(3),
                profile_maid_link_score = Some(0.5),
                total_profiles_per_maid = Some(2L),
                total_score_per_maid = Some(1.5),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(1.2),
                confidence_score = Some(0.6)
            ),
            GeoLocationToPosCustomer360Row(
                maid = "maid_2",
                maid_type = "maid_type_2",
                customer_360_id = "customer_2",
                frequency_linked = Some(4),
                profile_maid_link_score = Some(2.0),
                total_profiles_per_maid = Some(1L),
                total_score_per_maid = Some(5.0),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(10.0),
                confidence_score = Some(0.8)
            ),
            GeoLocationToPosCustomer360Row(
                maid = "maid_3",
                maid_type = "maid_type_3",
                customer_360_id = "customer_1",
                frequency_linked = Some(5),
                profile_maid_link_score = Some(0.7),
                total_profiles_per_maid = Some(4L),
                total_score_per_maid = Some(1.5),
                total_maids_per_profile = Some(2L),
                total_score_per_profile = Some(1.2),
                confidence_score = Some(0.7)
            )
        ).toDF

        val posCustomer360 = Seq(
            Customer360(
                customer_360_id = "customer_1",
                identities = Map("phone" -> Seq("111"), "email" -> Seq("first@example.com")),
                create_date = sqlDate("2022-06-10"),
                update_date = sqlDate("2022-06-14"),
                active_flag = true
            ),
            Customer360(
                customer_360_id = "customer_2",
                identities = Map("email" -> Seq("second@example.com")),
                create_date = sqlDate("2022-06-02"),
                update_date = sqlDate("2022-06-05"),
                active_flag = true
            )
        ).toDF

        // when

        val actual = createGeoIdentityRelationship(geoLocationToCustomer, posCustomer360, currentDate)(spark)

        // then

        val expected = Seq(
            IdentityRelationship(
                source = "maid_1",
                source_type = "maid_type_1",
                target = "111",
                target_type = "phone",
                relationship = RelationshipType,
                frequency = 3,
                created_date = currentDateSql,
                last_seen_date = currentDateSql,
                active_flag = true
            ),
            IdentityRelationship(
                source = "maid_1",
                source_type = "maid_type_1",
                target = "first@example.com",
                target_type = "email",
                relationship = RelationshipType,
                frequency = 3,
                created_date = currentDateSql,
                last_seen_date = currentDateSql,
                active_flag = true
            ),
            IdentityRelationship(
                source = "maid_2",
                source_type = "maid_type_2",
                target = "second@example.com",
                target_type = "email",
                relationship = RelationshipType,
                frequency = 4,
                created_date = currentDateSql,
                last_seen_date = currentDateSql,
                active_flag = true
            ),
            IdentityRelationship(
                source = "maid_3",
                source_type = "maid_type_3",
                target = "111",
                target_type = "phone",
                relationship = RelationshipType,
                frequency = 5,
                created_date = currentDateSql,
                last_seen_date = currentDateSql,
                active_flag = true
            ),
            IdentityRelationship(
                source = "maid_3",
                source_type = "maid_type_3",
                target = "first@example.com",
                target_type = "email",
                relationship = RelationshipType,
                frequency = 5,
                created_date = currentDateSql,
                last_seen_date = currentDateSql,
                active_flag = true
            )
        ).toDF

        assertDataFrameDataEquals(expected, actual)
        actual.schema.fieldNames shouldBe expected.schema.fieldNames
    }
}

object GeoIdentityRelationshipJobTest {

    private[GeoIdentityRelationshipJobTest] case class IntermediateGeoLocationToPosCustomer360Row(
        maid: String,
        maid_type: String,
        customer_360_id: String,
        frequency_linked: Option[Int],
        profile_maid_link_score: Option[Double]
    )

}

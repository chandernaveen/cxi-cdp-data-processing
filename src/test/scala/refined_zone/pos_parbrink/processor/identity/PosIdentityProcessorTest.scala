package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.{IdentityId, IdentityType}
import refined_zone.hub.identity.model.IdentityType.{CardHolderNameTypeNumber, Email, ParbrinkCustomerId, Phone}
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import refined_zone.pos_parbrink.processor.identity.PosIdentityProcessorTest.IdentitiesByOrder
import refined_zone.pos_parbrink.processor.CustomersProcessorTest.PrivacyLookup
import support.utils.DateTimeTestUtils.sqlDate
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class PosIdentityProcessorTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    val identities = Seq(
        ("ord_1", "loc_id_1", "hashed_email_1", Email.code, 3),
        ("ord_2", "loc_id_1", "hashed_email_2", Email.code, 3),
        ("ord_1", "loc_id_1", "hashed_phone_1", IdentityType.Phone.code, 3),
        ("ord_1", "loc_id_1", "hashed_phone_2", IdentityType.Phone.code, 3),
        ("ord_2", "loc_id_1", "cxi-partner-1_customer-1", ParbrinkCustomerId.code, 3),
        ("ord_1", "loc_id_1", "hashed_payment_details", IdentityType.CardHolderNameTypeNumber.code, 2),
        ("ord_1", "loc_id_2", "hashed_email_3", Email.code, 3)
    )

    test("add identities metadata") {

        // given
        val privacyLookup = Seq(
            PrivacyLookup(
                cxi_source = "cxi-test",
                identity_type = IdentityType.Phone.value,
                original_value = "12124567890",
                hashed_value = "hashed_phone_1",
                feed_date = sqlDate("2022-02-24"),
                run_id = "run_id_1"
            ),
            PrivacyLookup(
                cxi_source = "cxi-test",
                identity_type = Email.value,
                original_value = "someemail@google.com",
                hashed_value = "hashed_email_1",
                feed_date = sqlDate("2022-02-24"),
                run_id = "run_id_1"
            )
        ).toDF()

        val identitiesDf = identities.toDF("order_id", "location_id", CxiIdentityId, Type, Weight)

        // when
        val actual = PosIdentityProcessor.addCxiIdentitiesMetadata(privacyLookup, identitiesDf)

        // then
        withClue("identities with metadata do not match") {
            val expected = Seq(
                ("hashed_email_1", Email.code, 3, Map("email_domain" -> "google.com")),
                ("hashed_email_2", Email.code, 3, Map.empty[String, String]),
                ("hashed_phone_1", IdentityType.Phone.code, 3, Map("phone_area_code" -> "1212")),
                ("hashed_phone_2", IdentityType.Phone.code, 3, Map.empty[String, String]),
                ("cxi-partner-1_customer-1", ParbrinkCustomerId.code, 3, Map.empty[String, String]),
                ("hashed_payment_details", IdentityType.CardHolderNameTypeNumber.code, 2, Map.empty[String, String]),
                ("hashed_email_3", Email.code, 3, Map.empty[String, String])
            ).toDF(CxiIdentityId, Type, Weight, Metadata)
            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

    test("group identities by order") {

        // given
        val identitiesDf = identities.toDF("order_id", "location_id", CxiIdentityId, Type, Weight)

        // when
        val actual = PosIdentityProcessor.groupIdentitiesByOrder(identitiesDf)

        // then
        withClue("identities grouped by order do not match") {
            val expected = Seq(
                IdentitiesByOrder(
                    "ord_1",
                    "loc_id_1",
                    Seq(
                        IdentityId(Email.code, "hashed_email_1"),
                        IdentityId(Phone.code, "hashed_phone_1"),
                        IdentityId(Phone.code, "hashed_phone_2"),
                        IdentityId(CardHolderNameTypeNumber.code, "hashed_payment_details")
                    )
                ),
                IdentitiesByOrder(
                    "ord_2",
                    "loc_id_1",
                    Seq(
                        IdentityId(Email.code, "hashed_email_2"),
                        IdentityId(ParbrinkCustomerId.code, "cxi-partner-1_customer-1")
                    )
                ),
                IdentitiesByOrder("ord_1", "loc_id_2", Seq(IdentityId(Email.code, "hashed_email_3")))
            ).toDF()

            assert("Column size not Equal", expected.columns.size, actual.columns.size)
            assertDataFrameDataEquals(expected, actual)
        }

    }

}

object PosIdentityProcessorTest {

    case class IdentitiesByOrder(order_id: String, location_id: String, cxi_identity_ids: Seq[IdentityId])
}

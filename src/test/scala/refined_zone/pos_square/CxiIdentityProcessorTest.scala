package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.hub.identity.model.IdentityType.{CombinationBin, Email, Phone}
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import support.normalization.DateNormalization.parseToLocalDate
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class CxiIdentityProcessorTest extends BaseSparkBatchJobTest {

    test("Add cxiIdentities metadata") {
        import spark.implicits._

        // given
        val date = parseToLocalDate("2022-02-24")
        val allCustomersIds = Seq(
            ("ord_1", date, "loc_1", "email_hash_1", Email.value, "3"),
            ("ord_2", date, "loc_1", "email_hash_2", Email.value, "3"), // not exists in privacy table
            ("ord_3", date, "loc_1", "comb-bin_1", CombinationBin.value, "1"),
            ("ord_4", date, "loc_1", "phone_hash_1", Phone.value, "2")
        ).toDF("ord_id", "ord_timestamp", "ord_location_id", "cxi_identity_id", "type", "weight")

        val privacyTable = Seq(
            ("email_1@gmail.com", "email_hash_1", Email.value),
            ("1234567890", "phone_hash_1", Phone.value)
        ).toDF("original_value", "hashed_value", "identity_type")

        // when
        val actual = CxiIdentityProcessor.addCxiIdentitiesMetadata(privacyTable, allCustomersIds)

        // then
        withClue("Identities with metadata do not match") {
            val expected = Seq(
                ("email_hash_1", Email.value, "3", Map("email_domain" -> "gmail.com")),
                ("email_hash_2", Email.value, "3", Map.empty[String, String]),
                ("comb-bin_1", CombinationBin.value, "1", Map.empty[String, String]),
                ("phone_hash_1", Phone.value, "2", Map("phone_area_code" -> "1234"))
            ).toDF(CxiIdentityId, Type, Weight, Metadata)

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}

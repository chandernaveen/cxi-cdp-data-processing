package com.cxi.cdp.data_processing
package refined_zone.hub.identity.model

case class IdentityId(identity_type: String, cxi_identity_id: String)

object IdentityId {

    /** Used to determine which identity ID is source and which is target in identity_relationship table.
      *
      * In principle we do not care about the actual ordering as source-to-target relationship is bi-directional,
      * we just need this order to be defined.
      */
    val SourceToTargetOrdering: Ordering[IdentityId] = Ordering.by(r => (r.cxi_identity_id, r.identity_type))

}

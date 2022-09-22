package com.cxi.cdp.data_processing
package raw_zone.pos_omnivore.model

case class Payment(
    _embedded: PaymentEmbedded = null,
    _links: PaymentLinks = null,
    amount: Option[Long] = Option.empty,
    change: Option[Long] = Option.empty,
    comment: String = null,
    full_name: String = null,
    id: String = null,
    `type`: String = null,
    last4: String = null,
    tip: Option[Long] = Option.empty,
    omn_type: String = null
)

case class PaymentEmbedded(tender_type: TenderType = null)

case class TenderType(
    _links: SelfLink = null,
    allows_tips: Option[Boolean] = Option.empty,
    id: String = null,
    name: String = null,
    pos_id: String = null
)

case class PaymentLinks(tender_type: Link = null, self: Link = null)

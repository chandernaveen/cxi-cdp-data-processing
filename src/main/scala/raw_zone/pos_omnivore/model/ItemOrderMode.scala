package com.cxi.cdp.data_processing
package raw_zone.pos_omnivore.model

case class ItemOrderMode(
    _links: SelfLink,
    available: Option[Boolean] = Option.empty,
    id: String = null,
    name: String = null,
    pos_id: String = null
)

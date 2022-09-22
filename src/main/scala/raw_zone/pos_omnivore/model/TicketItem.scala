package com.cxi.cdp.data_processing
package raw_zone.pos_omnivore.model

case class TicketItem(
    _embedded: TicketItemEmbedded = null,
    _links: TicketItemLinks = null,
    id: String = null,
    included_tax: Option[Long] = Option.empty,
    name: String = null,
    price: Option[Long] = Option.empty,
    quantity: Option[Long] = Option.empty,
    seat: Option[Long] = Option.empty,
    sent: Option[Boolean] = Option.empty,
    sent_at: Option[Long] = Option.empty,
    split: Option[Long] = Option.empty
)

case class TicketItemEmbedded(
    discounts: Array[TicketItemDiscount] = null,
    item_order_mode: ItemOrderMode = null,
    menu_item: MenuItem = null,
    modifiers: Array[TicketItemModifier] = null
)

case class TicketItemDiscount(
    _embedded: TicketItemDiscountEmbedded = null,
    _links: TicketItemDiscountLinks = null,
    id: String = null,
    name: String = null,
    value: Option[Long] = Option.empty
)

case class TicketItemDiscountEmbedded(discount: Discount = null)

case class Discount(
    _links: SelfLink,
    applies_to: DiscountAppliesTo = null,
    available: Option[Boolean] = Option.empty,
    id: String = null,
    max_amount: Option[Long] = Option.empty,
    max_percentage: Option[Long] = Option.empty,
    min_amount: Option[Long] = Option.empty,
    min_percentage: Option[Long] = Option.empty,
    min_ticket_total: Option[Long] = Option.empty,
    name: String = null,
    open: Option[Boolean] = Option.empty,
    pos_id: String = null,
    omn_type: String = null,
    value: Option[Long] = Option.empty
)

case class DiscountAppliesTo(item: Option[Boolean] = Option.empty, ticket: Option[Boolean] = Option.empty)
case class TicketItemDiscountLinks(discount: Link = null, self: Link = null)

case class TicketItemModifier(
    _embedded: TicketItemModifierEmbedded = null,
    _link: TicketItemModifierLinks = null,
    comment: String = null,
    id: String = null,
    name: String = null,
    price: Option[Long] = Option.empty,
    quantity: Option[Long] = Option.empty
)

case class TicketItemModifierEmbedded(
    menu_modifier: MenuModifier = null
)

case class TicketItemModifierLinks(
    menu_modifier: Link = null,
    modifiers: Link = null,
    self: Link = null
)

case class TicketItemLinks(
    discounts: Link = null,
    item_order_mode: Link = null,
    menu_item: Link = null,
    modifiers: Link = null,
    self: Link = null
)

case class Link(href: String = null, omn_type: String = null)
case class SelfLink(self: Link = null)

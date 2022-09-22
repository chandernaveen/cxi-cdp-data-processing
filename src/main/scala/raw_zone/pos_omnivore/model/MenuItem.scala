package com.cxi.cdp.data_processing
package raw_zone.pos_omnivore.model

case class MenuItem(
    _embedded: MenuItemEmbedded = null,
    _links: MenuItemLinks = null,
    barcodes: Array[String] = null,
    id: String = null,
    in_stock: Option[Boolean] = Option.empty,
    name: String = null,
    open: Option[Boolean] = Option.empty,
    open_name: Option[Boolean] = Option.empty,
    pos_id: String = null,
    price_per_unit: Option[Long] = Option.empty
)

case class MenuItemEmbedded(
    menu_categories: Array[Category] = null,
    price_levels: Array[PriceLevel] = null
)

case class Category(
    _embedded: CategoryEmbedded = null,
    _links: CategoryLinks = null,
    id: String = null,
    level: Option[Long] = Option.empty,
    name: String = null,
    pos_id: String = null
)

case class CategoryEmbedded(
    menu_category_type: MenuCategoryType = null
)

case class MenuCategoryType(
    _links: SelfLink = null,
    id: String = null,
    name: String = null
)

case class CategoryLinks(
    menu_category_type: Link = null,
    parent_category: Link = null,
    self: Link = null
)

case class MenuModifierGroup(
    _embedded: MenuModifierGroupEmbedded = null,
    _links: MenuModifierGroupLinks = null,
    id: String = null,
    name: String = null,
    pos_id: String = null
)

case class MenuModifierGroupEmbedded(modifiers: MenuModifier = null)

case class MenuModifier(
    _embedded: MenuModifierEmbedded = null,
    _links: MenuModifierLinks = null,
    id: String = null,
    name: String = null,
    open: Option[Boolean] = Option.empty,
    pos_id: String = null,
    price_per_unit: Option[Long] = Option.empty
)

case class MenuModifierEmbedded(
    menu_categories: Array[Category] = null,
    price_levels: Array[PriceLevel] = null
)

case class MenuModifierLinks(
    menu_categories: Link = null,
    option_sets: Link = null,
    price_levels: Link = null,
    self: Link = null
)

case class MenuModifierGroupLinks(modifiers: Array[Link] = null, self: Link = null)

case class PriceLevel(
    _links: SelfLink = null,
    id: String = null,
    name: String = null,
    price_per_unit: Option[Long] = Option.empty
)

case class MenuItemLinks(
    menu_categories: Link = null,
    option_sets: Link = null,
    price_levels: Link = null,
    self: Link = null
)

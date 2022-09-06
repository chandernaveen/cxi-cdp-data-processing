package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.model

object ParbrinkRawModels {

    case class IncludedModifier(
        AutomaticallyAdd: Boolean,
        IsIncluded: Boolean,
        ItemId: Int,
        ModifierGroupId: Int,
        Position: Int,
        PrintInKitchen: Boolean
    )

    case class Item(ItemId: Int)

    case class OrderEntry(
        ItemId: Int,
        Id: Int,
        GrossSales: Double,
        Taxes: Seq[OrderEntryTax]
    )

    case class OrderEntryTax(
        Id: Int,
        Amount: Double,
        TaxId: Int
    )

    case class Payment(
        Id: Int,
        IsDeleted: Boolean,
        TenderId: Int,
        CardNumber: String,
        CardHolderName: String,
        TipAmount: Double
    )

    case class PhoneNumber(
        AreaCode: String,
        Extension: String,
        Id: Int,
        Number: String
    )
}

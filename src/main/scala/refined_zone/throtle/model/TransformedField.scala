package com.cxi.cdp.data_processing
package refined_zone.throtle.model

case class TransformedField(codeCategory: String, originalValue: String, transformedValue: String, isValid: Boolean) {
    override def toString: String = {
        s"{code_category: $codeCategory, code: $originalValue}"
    }
}

package com.cxi.cdp.data_processing
package support.normalization.udf

/** Common marker trait for classes that hold spark user-defined functions that intended for data normalization.
  * Specific classes should mix-in this trait.
  */
trait NormalizationUdfs extends Serializable

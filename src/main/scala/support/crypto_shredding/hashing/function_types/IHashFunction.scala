package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types

import org.apache.spark.sql.DataFrame

trait IHashFunction extends Serializable {
    def getType: String

    /**
     * @param originalDf original data frame with PII information
     * @return tuple where first (original) dataframe's PII columns are replaced with hashed values
     *         and second dataframe that contains extracted PII information along with hashes
     */
    def hash(originalDf: DataFrame): (DataFrame, DataFrame)
}

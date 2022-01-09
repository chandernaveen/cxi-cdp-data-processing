package com.cxi.cdp.data_processing
package support.packages.utils

import org.apache.commons.lang3.StringUtils

object PathUtils {

    final val PathDelimiter = "/"

    def concatPaths(parent: String, child: String): String = {
        StringUtils.removeEnd(parent, PathDelimiter) + PathDelimiter + StringUtils.removeStart(child, PathDelimiter)
    }

}

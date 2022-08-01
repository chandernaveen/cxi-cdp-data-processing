package com.cxi.cdp.data_processing
package support.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object DatalakeFiles {

    /** Provide a list of all files including those in sub-directories
      */
    def listAllFiles(hadoopConf: Configuration, path: String): Seq[String] = {
        def listRecursive(fs: FileSystem, path: Path): Seq[String] = {
            val files = fs.listStatus(path)
            if (files.isEmpty) {
                List()
            } else {
                files
                    .map(file => {
                        if (file.isDirectory) listRecursive(fs, file.getPath) else List(file.getPath.toString)
                    })
                    .reduce(_ ++ _)
            }
        }

        val fs: FileSystem = FileSystem.get(hadoopConf)
        try {
            listRecursive(fs, new Path(path))
        } finally {
            fs.close()
        }
    }
}

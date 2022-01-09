package com.cxi.cdp.data_processing
package support.packages.utils

import org.scalatest.{FunSuite, Matchers}

class PathUtilsTest extends FunSuite with Matchers  {

    test("concatPaths") {
        PathUtils.concatPaths("/a/b", "c/d") shouldBe "/a/b/c/d"
        PathUtils.concatPaths("/a/b/", "c/d") shouldBe "/a/b/c/d"
        PathUtils.concatPaths("/a/b", "/c/d") shouldBe "/a/b/c/d"
        PathUtils.concatPaths("/a/b/", "/c/d") shouldBe "/a/b/c/d"
    }

}

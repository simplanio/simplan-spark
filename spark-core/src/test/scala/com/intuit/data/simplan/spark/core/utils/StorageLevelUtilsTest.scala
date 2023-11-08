package com.intuit.data.simplan.spark.core.utils

import com.intuit.data.simplan.spark.core.domain.operator.config.SparkStorageLevel
import org.scalatest.funsuite.AnyFunSuiteLike

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 28-Sep-2023 at 12:26 AM
  */
class StorageLevelUtilsTest extends AnyFunSuiteLike {

  test("Storage Test Mapper"){
    val storageLevel = SparkStorageLevel.MEMORY_ONLY
    val storageLevelMapper = StorageLevelUtils.storageLevelMapper(storageLevel)
    assert(storageLevelMapper == org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
  }
}

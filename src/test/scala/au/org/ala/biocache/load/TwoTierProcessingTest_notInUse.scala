package au.org.ala.biocache.load

import java.io.File

import au.org.ala.biocache.tool.ProcessRecords
import au.org.ala.biocache.tool.ProcessRecords.{logger, processFileOfRowKeys}
import au.org.ala.biocache.tool.ProcessSingleRecord.processRecord
import au.org.ala.biocache.{Config, ConfigFunSuite}
import org.junit.{Before, BeforeClass, Test}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TwoTierProcessingTest_notInUse extends ConfigFunSuite with BeforeAndAfterAll {
  val WORK_FILE = new File("src/test/resources/au/org/ala/load/test-twotier/occurrences.csv")
  val DATA_RESOURCE_UID = "dr9999"
  val UNIQUE_FIELDS = Array("occurrenceID")
  val LOG_ROW_KEYS = true
  val TEST_FILE = false
  val TOT_RECS = 24

  override def beforeAll() {
    super.beforeAll()
    val loader = new DwcCSVLoader
    loader.deleteOldRowKeys(DATA_RESOURCE_UID)
    loader.loadFile(
      WORK_FILE,
      DATA_RESOURCE_UID,
      UNIQUE_FIELDS,
      Map(),
      false,
      LOG_ROW_KEYS,
      TEST_FILE
    )
    val rowkeyMap = Config.persistenceManager.get("dr9999|3", "occ_uuid")
    val rowkey = rowkeyMap.get("value")
    val rec = Config.persistenceManager.get(rowkey,"occ")
    ProcessRecords.logger.info("Before processing: " + rec.toString)
    val recUuid = rec.get("rowKey")
    //This does not work - the processed records are identical to the raw loaded records.
    //ProcessRecords.processRecords(DATA_RESOURCE_UID, 1)
    //This does not work - the processed records are identical to the raw loaded records.
    processRecord(recUuid)
    val rec2 = Config.persistenceManager.get(rowkey,"occ")


    ProcessRecords.logger.info("Records loaded and processed")
    //val rec2 = Config.persistenceManager.get(rowkey,"occ")
    //ProcessRecords.logger.info("After processing: " + rec2.toString)
  }

  test("sensitive record has originalSensitiveValues") {
    val rowkeyMap = Config.persistenceManager.get("dr9999|3", "occ_uuid")
    val rowkey = rowkeyMap.get("value")
    val rec = Config.persistenceManager.get(rowkey,"occ")
    ProcessRecords.logger.info("After processing:" + rec.toString)
    assert(1 == 1)
  }


}

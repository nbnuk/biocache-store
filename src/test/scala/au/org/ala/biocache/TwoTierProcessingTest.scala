package au.org.ala.biocache

import java.io.File

import au.org.ala.biocache.caches.{SensitivityDAO, SpatialLayerDAO}
import au.org.ala.biocache.load.{DwcCSVLoader, FullRecordMapper}
import au.org.ala.biocache.model.{FullRecord, Versions}
import au.org.ala.biocache.processor.{ClassificationProcessor, DefaultValuesProcessor, EventProcessor, LocationProcessor, LocationTwoTierPreProcessor, RecordProcessor, SensitivityProcessor}
import org.apache.commons.lang.StringUtils
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.tool.ProcessLocalRecords
import au.org.ala.biocache.util.Json

/**
  * Performs some Location Processing tests
  */
@RunWith(classOf[JUnitRunner])
class TwoTierProcessingTest extends ConfigFunSuite with BeforeAndAfterAll {

  val WORK_FILE = new File("src/test/resources/au/org/ala/load/test-twotier/occurrences.csv")
  val DATA_RESOURCE_UID = "dr9999"
  val UNIQUE_FIELDS = Array("occurrenceID")
  val LOG_ROW_KEYS = true
  val TEST_FILE = false

  test("NBNtoBlur - transfer data to high resolution fields"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.rowKey = "1"
    raw.occurrence.highResolution = "1"
    raw.occurrence.highResolutionNBNtoBlur = "1"
    raw.location.decimalLatitude = "55.12345"
    raw.location.decimalLongitude = "-1.23456"
    raw.location.coordinateUncertaintyInMeters = "5"
    raw.location.gridReference = "NZ4891381275"
    raw.location.gridSizeInMeters = "10"
    raw.location.locality = "High res locality"
    raw.event.eventID = "High res event"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)

    expectResult("55.12345") {
      raw.location.highResolutionDecimalLatitude
    }
    expectResult("-1.23456") {
      raw.location.highResolutionDecimalLongitude
    }
    expectResult("5") {
      raw.location.highResolutionCoordinateUncertaintyInMeters
    }
    expectResult("NZ4891381275") {
      raw.location.highResolutionGridReference
    }
    expectResult("10") {
      raw.location.highResolutionGridSizeInMeters
    }
    expectResult("High res locality") {
      raw.location.highResolutionLocality
    }
    expectResult("High res event") {
      raw.event.highResolutionEventID
    }
  }



  test("NBNtoBlur - generate blurred data (assuming 1km blurring)"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.rowKey = "1"
    raw.occurrence.highResolution = "t"
    raw.occurrence.highResolutionNBNtoBlur = "t"
    raw.location.decimalLatitude = "55.12345"
    raw.location.decimalLongitude = "-1.23456"
    raw.location.coordinateUncertaintyInMeters = "5"
    raw.location.gridReference = "NZ4891381275"
    raw.location.gridSizeInMeters = "10"
    raw.location.locality = "High res locality"
    raw.event.eventID = "High res event"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)

    expectResult("55.12551") { //centroid, not rounded
      raw.location.decimalLatitude
    }
    expectResult("-1.24099") { //centroid, not rounded
      raw.location.decimalLongitude
    }
    expectResult("707.1") {
      raw.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ4881") {
      raw.location.gridReference
    }
    expectResult("1000") {
      raw.location.gridSizeInMeters
    }
    expectResult(null) {
      raw.location.locality
    }
    expectResult(null) {
      raw.event.eventID
    }
    val originalBlurredValues = raw.occurrence.originalBlurredValues

    expectResult("55.12551") {
      originalBlurredValues("decimalLatitude")
    }
    expectResult("-1.24099") {
      originalBlurredValues("decimalLongitude")
    }
    expectResult("707.1") {
      originalBlurredValues("coordinateUncertaintyInMeters")
    }
    expectResult("NZ4881") {
      originalBlurredValues("gridReference")
    }
    expectResult("1000") {
      originalBlurredValues("gridSizeInMeters")
    }
    expectResult(null) {
      originalBlurredValues.getOrElse("locality", null)
    }
    expectResult(null) {
      originalBlurredValues.getOrElse("eventID", null)
    }
  }

  test("NBNtoBlur - is already adequately blurred - leave unchanged") {
    val raw = new FullRecord
    val processed = new FullRecord

    raw.rowKey = "1"
    raw.occurrence.highResolution = "t"
    raw.occurrence.highResolutionNBNtoBlur = "t"
    raw.classification.taxonID = "NBNSYS0000000136"
    raw.classification.scientificName = "Anas querquedula"
    raw.location.decimalLatitude = "52.7"
    raw.location.decimalLongitude = "-2.9"
    raw.location.coordinateUncertaintyInMeters = "5000"
    raw.location.gridSizeInMeters = "10000"
    raw.location.gridReference = "SJ41"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)

    expectResult("52.7") {
      raw.location.decimalLatitude
    }
    expectResult("-2.9") {
      raw.location.decimalLongitude
    }
    expectResult("5000") {
      raw.location.coordinateUncertaintyInMeters
    }
    expectResult("SJ41") {
      raw.location.gridReference
    }
    expectResult("10000") {
      raw.location.gridSizeInMeters
    }
    expectResult(null) {
      raw.location.locality
    }
    expectResult(null) {
      raw.event.eventID
    }
    val originalBlurredValues = raw.occurrence.originalBlurredValues

    expectResult("52.7") {
      originalBlurredValues("decimalLatitude")
    }
    expectResult("-2.9") {
      originalBlurredValues("decimalLongitude")
    }
    expectResult("5000") {
      originalBlurredValues("coordinateUncertaintyInMeters")
    }
    expectResult("SJ41") {
      originalBlurredValues("gridReference")
    }
    expectResult("10000") {
      originalBlurredValues("gridSizeInMeters")
    }
    expectResult(null) {
      originalBlurredValues.getOrElse("locality", null)
    }
    expectResult(null) {
      originalBlurredValues.getOrElse("eventID", null)
    }
  }

  test("Store high res data into originalSensitiveValues") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.rowKey = "1"
    raw.occurrence.highResolution = "t"
    raw.location.highResolutionDecimalLatitude = "55.12345"
    raw.location.highResolutionDecimalLongitude = "-1.23456"
    raw.location.highResolutionCoordinateUncertaintyInMeters = "5"
    raw.location.highResolutionGridReference = "NZ4891381275"
    raw.location.highResolutionGridSizeInMeters = "10"
    raw.location.highResolutionLocality = "High res locality"
    raw.event.highResolutionEventID = "High res event"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)

    val originalSensitiveValues = raw.occurrence.originalSensitiveValues

    expectResult("55.12345") {
      originalSensitiveValues("decimalLatitude")
    }
    expectResult("-1.23456") {
      originalSensitiveValues("decimalLongitude")
    }
    expectResult("5") {
      originalSensitiveValues("coordinateUncertaintyInMeters")
    }
    expectResult("NZ4891381275") {
      originalSensitiveValues("gridReference")
    }
    expectResult("10") {
      originalSensitiveValues("gridSizeInMeters")
    }
    expectResult("High res locality") {
      originalSensitiveValues.getOrElse("locality", null)
    }
    expectResult("High res event") {
      originalSensitiveValues.getOrElse("eventID", null)
    }
  }

  test("Process location info") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.rowKey = "1"
    raw.occurrence.highResolution = "1"
    raw.occurrence.highResolutionNBNtoBlur = "1"
    raw.location.decimalLatitude = "55.12345"
    raw.location.decimalLongitude = "-1.23456"
    raw.location.coordinateUncertaintyInMeters = "5"
    raw.location.gridReference = "NZ4891381275"
    raw.location.gridSizeInMeters = "10"
    raw.location.locality = "High res locality"
    raw.event.eventID = "High res event"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)
    (new LocationProcessor).process("test", raw, processed)

    expectResult("55.12551") { //centroid, not rounded
      processed.location.decimalLatitude
    }
    expectResult("-1.24099") { //centroid, not rounded
      processed.location.decimalLongitude
    }
    expectResult("707.1") {
      processed.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ4881") {
      processed.location.gridReference
    }
    expectResult("1000") {
      processed.location.gridSizeInMeters
    }
    expectResult(null) {
      processed.location.locality
    }
    expectResult(null) {
      processed.event.eventID
    }
  }

  test("Sensitive processing - leaves non-sensitive record unchanged") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.rowKey = "1"
    raw.occurrence.highResolution = "1"
    raw.occurrence.highResolutionNBNtoBlur = "1"
    raw.location.decimalLatitude = "55.12345"
    raw.location.decimalLongitude = "-1.23456"
    raw.location.coordinateUncertaintyInMeters = "5"
    raw.location.gridReference = "NZ4891381275"
    raw.location.gridSizeInMeters = "10"
    raw.location.locality = "High res locality"
    raw.event.eventID = "High res event"

    raw.classification.scientificName = "Vulpes vulpes"
    raw.classification.taxonID = "NHMSYS0000080188"

    //note that this is done in a clumsy way below, because
    /* this does not set the processed values for some reason
    val loader = new DwcCSVLoader
    //new DeleteLocalDataResource().delete("occ", 1, Array(DATA_RESOURCE_UID), 0)
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

    val checkpointFile = Config.tmpWorkDir + "/process-local-records-checkpoints.txt"
    val rp = new ProcessLocalRecords
    rp.processRecords(1, List("dr9999"), List(), 0, checkpointFile)
    val pm = Config.persistenceManager
    val rowkeyMap = Config.persistenceManager.get("dr9999|1", "occ_uuid")
    val rowkey = rowkeyMap.get("value")
    val occ = Config.persistenceManager.get(rowkey,"occ")
    */
    (new LocationTwoTierPreProcessor).process("test", raw, processed)
    (new LocationProcessor).process("test", raw, processed)
    (new SensitivityProcessor).process("test", raw, processed)

    expectResult("55.12551") { //centroid, not rounded
      processed.location.decimalLatitude
    }
    expectResult("-1.24099") { //centroid, not rounded
      processed.location.decimalLongitude
    }
    expectResult("707.1") {
      processed.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ4881") {
      processed.location.gridReference
    }
    expectResult("1000") {
      processed.location.gridSizeInMeters
    }
    expectResult(null) {
      processed.location.locality
    }
    expectResult(null) {
      processed.event.eventID
    }
  }


  test("Sensitive processing - record is already adequately blurred") {
    val raw = new FullRecord
    val processed = new FullRecord

    raw.rowKey = "1"
    raw.location.country = "United Kingdom"
    raw.location.stateProvince = "England"

    raw.occurrence.highResolution = "t"
    raw.occurrence.highResolutionNBNtoBlur = "t"
    raw.classification.taxonID = "NBNSYS0000000136"
    raw.classification.scientificName = "Anas querquedula"
    raw.location.decimalLatitude = "52.7"
    raw.location.decimalLongitude = "-2.9"
    raw.location.coordinateUncertaintyInMeters = "5000"
    raw.location.gridReference = "SJ31"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)
    (new LocationProcessor).process("test", raw, processed)
    (new SensitivityProcessor).process("test", raw, processed)

    expectResult("52.7") {
      processed.location.decimalLatitude
    }
    expectResult("-2.9") {
      processed.location.decimalLongitude
    }
    expectResult("5000.0") {
      processed.location.coordinateUncertaintyInMeters
    }
    expectResult("SJ31") {
      processed.location.gridReference
    }
    expectResult("10000") {
      processed.location.gridSizeInMeters
    }

  }

  test("Sensitive processing - record must be blurred") {
    val raw = new FullRecord
    val processed = new FullRecord

    raw.rowKey = "1"
    //needed for sensitivity processor
    raw.location.country = "United Kingdom"
    raw.location.stateProvince = "England"

    raw.occurrence.highResolution = "t"
    raw.occurrence.highResolutionNBNtoBlur = "t"
    raw.classification.taxonID = "NHMSYS0021276106" //"NBNSYS0000000136"
    raw.classification.scientificName = "Spatula querquedula" //Anas querquedula
    raw.location.decimalLatitude = "52.712"
    raw.location.decimalLongitude = "-2.756"
    raw.location.coordinateUncertaintyInMeters = "500"
    raw.location.gridReference = "SJ4913"
    raw.location.gridSizeInMeters = "1000"

    (new LocationTwoTierPreProcessor).process("test", raw, processed)
    (new LocationProcessor).process("test", raw, processed)
    (new SensitivityProcessor).process("test", raw, processed)

    val originalBlurredValues = raw.occurrence.originalBlurredValues
    val originalSensitiveValues = raw.occurrence.originalSensitiveValues

    expectResult("500") {
      originalSensitiveValues("coordinateUncertaintyInMeters")
    }
    expectResult("52.712") {
      originalSensitiveValues("decimalLatitude")
    }
    expectResult("-2.756") {
      originalSensitiveValues("decimalLongitude")
    }
    expectResult("SJ4913") {
      originalSensitiveValues("gridReference")
    }
    expectResult("1000") {
      originalSensitiveValues("gridSizeInMeters")
    }

    expectResult("707.1") {
      originalBlurredValues("coordinateUncertaintyInMeters")
    }
    expectResult("52.71679") { //centroid of 1km grid
       originalBlurredValues("decimalLatitude")
    }
    expectResult("-2.74903") { //centroid of 1km grid
      originalBlurredValues("decimalLongitude")
    }
    expectResult("SJ4913") {
      originalBlurredValues("gridReference")
    }
    expectResult("1000") {
      originalBlurredValues("gridSizeInMeters")
    }

    expectResult("52.7") { //should really be centroid of 10km grid?
      processed.location.decimalLatitude
    }
    expectResult("-2.7") {
      processed.location.decimalLongitude
    }
    expectResult("7071.1") {
      processed.location.coordinateUncertaintyInMeters
    }
    expectResult("SJ41") {
      processed.location.gridReference
    }
    expectResult("10000") {
      processed.location.gridSizeInMeters
    }
  }


}

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

  val rawSensitive = new FullRecord
  val processedSensitive = new FullRecord
  val rawNotSensitive = new FullRecord
  val processedNotSensitive = new FullRecord
  val rawSensitiveCrude = new FullRecord
  val processedSensitiveCrude = new FullRecord
  val rawSensitiveDataProviderBlurred = new FullRecord
  val processedSensitiveDataProviderBlurred = new FullRecord


  override def beforeAll:Unit = {
    rawSensitive.rowKey = "1"
    rawSensitive.occurrence.highResolution = "1"
    rawSensitive.occurrence.highResolutionNBNtoBlur = "1"
    rawSensitive.location.decimalLatitude = "55.12345"
    rawSensitive.location.decimalLongitude = "-1.23456"
    rawSensitive.location.coordinateUncertaintyInMeters = "5"
    rawSensitive.location.gridReference = "NZ4891381275"
    rawSensitive.location.gridSizeInMeters = "10"
    rawSensitive.location.locality = "High res locality"
    rawSensitive.event.eventID = "High res event"
    rawSensitive.classification.taxonID = "NBNSYS0000000136"
    rawSensitive.classification.scientificName = "Anas querquedula"

    (new LocationTwoTierPreProcessor).process("test", rawSensitive, processedSensitive)
    (new LocationProcessor).process("test", rawSensitive, processedSensitive)

    rawSensitiveCrude.rowKey = "1"
    rawSensitiveCrude.occurrence.highResolution = "t"
    rawSensitiveCrude.occurrence.highResolutionNBNtoBlur = "t"
    rawSensitiveCrude.location.decimalLatitude = "52.7"
    rawSensitiveCrude.location.decimalLongitude = "-2.9"
    rawSensitiveCrude.location.coordinateUncertaintyInMeters = "5000"
    rawSensitiveCrude.location.gridReference = "SJ41"
    rawSensitiveCrude.location.gridSizeInMeters = "10000"
    rawSensitiveCrude.location.locality = "High res locality"
    rawSensitiveCrude.event.eventID = "High res event"
    rawSensitiveCrude.classification.taxonID = "NBNSYS0000000136"
    rawSensitiveCrude.classification.scientificName = "Anas querquedula"

    (new LocationTwoTierPreProcessor).process("test", rawSensitiveCrude, processedSensitiveCrude)
    (new LocationProcessor).process("test", rawSensitiveCrude, processedSensitiveCrude)
    (new SensitivityProcessor).process("test", rawSensitiveCrude, processedSensitiveCrude)

    rawNotSensitive.rowKey = "1"
    rawNotSensitive.occurrence.highResolution = "1"
    rawNotSensitive.occurrence.highResolutionNBNtoBlur = "1"
    rawNotSensitive.location.decimalLatitude = "55.12345"
    rawNotSensitive.location.decimalLongitude = "-1.23456"
    rawNotSensitive.location.coordinateUncertaintyInMeters = "5"
    rawNotSensitive.location.gridReference = "NZ4891381275"
    rawNotSensitive.location.gridSizeInMeters = "10"
    rawNotSensitive.location.locality = "High res locality"
    rawNotSensitive.event.eventID = "High res event"
    rawNotSensitive.classification.scientificName = "Vulpes vulpes"
    rawNotSensitive.classification.taxonID = "NHMSYS0000080188"

    (new LocationTwoTierPreProcessor).process("test", rawNotSensitive, processedNotSensitive)
    (new LocationProcessor).process("test", rawNotSensitive, processedNotSensitive)
    (new SensitivityProcessor).process("test", rawNotSensitive, processedNotSensitive)

    rawSensitiveDataProviderBlurred.rowKey = "1"
    rawSensitiveDataProviderBlurred.occurrence.highResolution = "1"
    rawSensitiveDataProviderBlurred.location.highResolutionDecimalLatitude = "55.12345"
    rawSensitiveDataProviderBlurred.location.highResolutionDecimalLongitude = "-1.23456"
    rawSensitiveDataProviderBlurred.location.highResolutionCoordinateUncertaintyInMeters = "5"
    rawSensitiveDataProviderBlurred.location.highResolutionGridReference = "NZ4891381275"
    rawSensitiveDataProviderBlurred.location.highResolutionGridSizeInMeters = "10"
    rawSensitiveDataProviderBlurred.location.highResolutionLocality = "High res locality"
    rawSensitiveDataProviderBlurred.event.highResolutionEventID = "High res event"
    rawSensitiveDataProviderBlurred.classification.taxonID = "NBNSYS0000000136"
    rawSensitiveDataProviderBlurred.classification.scientificName = "Anas querquedula"
    rawSensitiveDataProviderBlurred.location.decimalLatitude = "55.12551"
    rawSensitiveDataProviderBlurred.location.decimalLongitude = "-1.24099"
    rawSensitiveDataProviderBlurred.location.coordinateUncertaintyInMeters = "707.1"
    rawSensitiveDataProviderBlurred.location.gridReference = "NZ4881"
    rawSensitiveDataProviderBlurred.location.gridSizeInMeters = "1000"
    rawSensitiveDataProviderBlurred.location.locality = null
    rawSensitiveDataProviderBlurred.event.eventID = null

    (new LocationTwoTierPreProcessor).process("test", rawSensitiveDataProviderBlurred, processedSensitiveDataProviderBlurred)
    (new LocationProcessor).process("test", rawSensitiveDataProviderBlurred, processedSensitiveDataProviderBlurred)
  }

  test("NBNtoBlur - transfer data to high resolution fields"){

    expectResult("55.12345") {
      rawSensitive.location.highResolutionDecimalLatitude
    }
    expectResult("-1.23456") {
      rawSensitive.location.highResolutionDecimalLongitude
    }
    expectResult("5") {
      rawSensitive.location.highResolutionCoordinateUncertaintyInMeters
    }
    expectResult("NZ4891381275") {
      rawSensitive.location.highResolutionGridReference
    }
    expectResult("10") {
      rawSensitive.location.highResolutionGridSizeInMeters
    }
    expectResult("High res locality") {
      rawSensitive.location.highResolutionLocality
    }
    expectResult("High res event") {
      rawSensitive.event.highResolutionEventID
    }
  }

  test("NBNtoBlur - generate blurred data (assuming 1km blurring)"){

    expectResult("55.12551") { //centroid, not rounded
      rawSensitive.location.decimalLatitude
    }
    expectResult("-1.24099") { //centroid, not rounded
      rawSensitive.location.decimalLongitude
    }
    expectResult("707.1") {
      rawSensitive.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ4881") {
      rawSensitive.location.gridReference
    }
    expectResult("1000") {
      rawSensitive.location.gridSizeInMeters
    }
    expectResult(null) {
      rawSensitive.location.locality
    }
    expectResult(null) {
      rawSensitive.event.eventID
    }
    val originalBlurredValues = rawSensitive.occurrence.originalBlurredValues

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


    expectResult("52.7") {
      rawSensitiveCrude.location.decimalLatitude
    }
    expectResult("-2.9") {
      rawSensitiveCrude.location.decimalLongitude
    }
    expectResult("5000") {
      rawSensitiveCrude.location.coordinateUncertaintyInMeters
    }
    expectResult("SJ41") {
      rawSensitiveCrude.location.gridReference
    }
    expectResult("10000") {
      rawSensitiveCrude.location.gridSizeInMeters
    }
    expectResult(null) {
      rawSensitiveCrude.location.locality
    }
    expectResult(null) {
      rawSensitiveCrude.event.eventID
    }
    val originalBlurredValues = rawSensitiveCrude.occurrence.originalBlurredValues

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


    val originalSensitiveValues = rawSensitive.occurrence.originalSensitiveValues

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

    expectResult("55.12551") { //centroid, not rounded
      processedSensitive.location.decimalLatitude
    }
    expectResult("-1.24099") { //centroid, not rounded
      processedSensitive.location.decimalLongitude
    }
    expectResult("707.1") {
      processedSensitive.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ4881") {
      processedSensitive.location.gridReference
    }
    expectResult("1000") {
      processedSensitive.location.gridSizeInMeters
    }
    expectResult(null) {
      processedSensitive.location.locality
    }
    expectResult(null) {
      processedSensitive.event.eventID
    }
  }

  test("Sensitive processing - leaves non-sensitive record unchanged") {


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


    expectResult("55.12551") { //centroid, not rounded
      processedNotSensitive.location.decimalLatitude
    }
    expectResult("-1.24099") { //centroid, not rounded
      processedNotSensitive.location.decimalLongitude
    }
    expectResult("707.1") {
      processedNotSensitive.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ4881") {
      processedNotSensitive.location.gridReference
    }
    expectResult("1000") {
      processedNotSensitive.location.gridSizeInMeters
    }
    expectResult(null) {
      processedNotSensitive.location.locality
    }
    expectResult(null) {
      processedNotSensitive.event.eventID
    }
  }

  test("Sensitive processing - record is already adequately blurred") {


    expectResult("52.7") {
      processedSensitiveCrude.location.decimalLatitude
    }
    expectResult("-2.9") {
      processedSensitiveCrude.location.decimalLongitude
    }
    expectResult("5000.0") {
      processedSensitiveCrude.location.coordinateUncertaintyInMeters
    }
    expectResult("SJ31") {
      processedSensitiveCrude.location.gridReference
    }
    expectResult("10000") {
      processedSensitiveCrude.location.gridSizeInMeters
    }

  }

  test("Sensitive processing - record must be blurred") {


    (new SensitivityProcessor).process("test", rawSensitive, processedSensitive)

    val originalBlurredValues = rawSensitive.occurrence.originalBlurredValues
    val originalSensitiveValues = rawSensitive.occurrence.originalSensitiveValues

    expectResult("707.1") {
      originalBlurredValues("coordinateUncertaintyInMeters")
    }
    expectResult("55.12551") { //centroid of 1km grid
       originalBlurredValues("decimalLatitude")
    }
    expectResult("-1.24099") { //centroid of 1km grid
      originalBlurredValues("decimalLongitude")
    }
    expectResult("NZ4881") {
      originalBlurredValues("gridReference")
    }
    expectResult("1000") {
      originalBlurredValues("gridSizeInMeters")
    }

    expectResult("55.1") { //should really be centroid of 10km grid?
      processedSensitive.location.decimalLatitude
    }
    expectResult("-1.2") {
      processedSensitive.location.decimalLongitude
    }
    expectResult("7071.1") {
      processedSensitive.location.coordinateUncertaintyInMeters
    }
    expectResult("NZ48") {
      processedSensitive.location.gridReference
    }
    expectResult("10000") {
      processedSensitive.location.gridSizeInMeters
    }
  }


}

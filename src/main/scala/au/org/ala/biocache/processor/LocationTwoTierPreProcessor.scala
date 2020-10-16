package au.org.ala.biocache.processor

import au.org.ala.biocache._
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model._
import au.org.ala.biocache.util.GridUtil.{getGridFromLatLonAndGridSize, getGridRefAsResolutions, getGridSizeFromCoordinateUncertainty, getGridSizeInMeters, gridReferenceToEastingNorthing, irishGridReferenceToEastingNorthing, logger, osGridReferenceToEastingNorthing}
import au.org.ala.biocache.util.{GISUtil, GridUtil, Json, StringHelper}
import au.org.ala.biocache.vocab._
import org.apache.commons.math3.util.Precision
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ArrayBuffer

/**
  * Pre-processor of two-tier higher resolution location information.
  * This primarily manages NBN blurring (as opposed to explicitly stated low-resolution values),
  * and populates the originalSensitiveValues (high-resolution) and originalBlurredValues (low-resolution) fields.
  */
class LocationTwoTierPreProcessor extends Processor {

  import AssertionCodes._
  import AssertionStatus._
  import StringHelper._

  val logger = LoggerFactory.getLogger("LocationTwoTierPreProcessor")

  val locProcessor = new LocationProcessor

  val NBN_BLUR_GRID_SIZE = 1000 //this needs to be a canonical grid size, i.e. 1,10,100,1000,2000,10000,50000,100000
  /**
    * Process highresolutionnbntoblur
    * if this is true then
    *   (a) copy data from $ fields to highresolution$ fields
    *   (b) apply blurring to original $ fields
    * otherwise the $ field are presumed to be blurred and the highreoslution$ fields are high-resolution
    */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    logger.debug(s"Pre-processing NBN Two-tier location for guid: $guid")

    val assertions = new ArrayBuffer[QualityAssertion]

    if (raw.occurrence.highResolution != null) {
      if (raw.occurrence.highResolution.toLowerCase() == "true" || raw.occurrence.highResolution.toLowerCase() == "1" || raw.occurrence.highResolution.toLowerCase() == "t") {

        if (raw.occurrence.highResolutionNBNtoBlur != null) {
          if (raw.occurrence.highResolutionNBNtoBlur.toLowerCase() == "true" || raw.occurrence.highResolutionNBNtoBlur.toLowerCase() == "1" || raw.occurrence.highResolutionNBNtoBlur.toLowerCase() == "t") {
            copyToHighRes(raw, assertions)
            fillOutHighResFields(raw, assertions)
            if (raw.occurrence.originalBlurredValues == null || raw.occurrence.originalBlurredValues.isEmpty) {
              blur(raw, assertions)
              packOriginalBlurredValues(raw, assertions)
            } else {
              //already stored the first time this record was processed
              unpackOriginalBlurredValues(raw, assertions)
            }
          }
        } else {
          fillOutHighResFields(raw, assertions)
          if (raw.occurrence.originalBlurredValues == null || raw.occurrence.originalBlurredValues.isEmpty) {
            packOriginalBlurredValues(raw, assertions)
          }
        }
        //do this regardless of whether sensitive values exist, since might need to overwrite originalSensitiveValues for a sensitive record which has now been loaded with high resolution information
        packOriginalHighResolutionValues(raw, assertions)

        val fieldsToUpdate = getFieldsToUpdateMap(raw)
        Config.persistenceManager.put(raw.rowKey, "occ", fieldsToUpdate, false, false)
      }
    }

    //return the assertions created by this processor
    assertions.toArray
  }

  /**
    * Copy $ fields to highresolution$ fields
    * @param raw
    * @param assertions
    */
  private def copyToHighRes(raw: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {
    raw.location.highResolutionDecimalLatitude = raw.location.decimalLatitude
    raw.location.highResolutionDecimalLongitude = raw.location.decimalLongitude
    raw.location.highResolutionGridReference = raw.location.gridReference
    raw.location.highResolutionGridSizeInMeters = raw.location.gridSizeInMeters
    raw.location.highResolutionCoordinateUncertaintyInMeters = raw.location.coordinateUncertaintyInMeters
    raw.location.highResolutionLocality = raw.location.locality
    raw.event.highResolutionEventID = raw.event.eventID
  }

  /**
    * Populate (high-resolution) gridReference, gridSizeInMeters and/or coordinateUncertaintyInMeters depending on which
    * other values have been supplied.
    * @param raw
    * @param assertions
    * Note: This is inconsistent with the sensitive data processing, which will populate processed fields but leave the
    * raw ones null, e.g. gridSizeInMeters_p if the raw gridSizeInMeters is missing. Indexing should use whichever is
    * populated, so this may not be an issue
    */
  private def fillOutHighResFields(raw: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {

    if (raw.location.highResolutionGridReference == null || raw.location.highResolutionGridReference.length == 0) {
      if (raw.location.highResolutionDecimalLatitude != null && raw.location.highResolutionDecimalLatitude.length != 0 &&
        raw.location.highResolutionDecimalLongitude != null && raw.location.highResolutionDecimalLongitude.length != 0) {

        if (raw.location.highResolutionGridSizeInMeters != null && raw.location.highResolutionGridSizeInMeters.length > 0) {
          val grid = getGridFromLatLonAndGridSize(raw.location.highResolutionDecimalLatitude.toFloat, raw.location.highResolutionDecimalLongitude.toFloat, raw.location.highResolutionGridSizeInMeters, raw.location.stateProvince, raw.location.geodeticDatum)
          raw.location.highResolutionGridReference = grid.getOrElse(null)
        } else if (raw.location.highResolutionCoordinateUncertaintyInMeters != null && raw.location.highResolutionCoordinateUncertaintyInMeters.length > 0) {
          val gridSizeToUse = getGridSizeFromCoordinateUncertainty(raw.location.highResolutionCoordinateUncertaintyInMeters.toFloat)
          if (gridSizeToUse != 0) {
            val grid = getGridFromLatLonAndGridSize(raw.location.highResolutionDecimalLatitude.toFloat, raw.location.highResolutionDecimalLongitude.toFloat, gridSizeToUse.toString, raw.location.stateProvince, raw.location.geodeticDatum)
            raw.location.highResolutionGridReference = grid.getOrElse(null)
          }
        }
      }
    }

    if (raw.location.highResolutionGridSizeInMeters == null || raw.location.highResolutionGridSizeInMeters.length == 0) {
      raw.location.highResolutionGridSizeInMeters = getGridSizeInMeters(raw.location.highResolutionGridReference).getOrElse(null).toString
    }

    if (raw.location.highResolutionCoordinateUncertaintyInMeters == null || raw.location.highResolutionCoordinateUncertaintyInMeters.length == 0) {
      if (raw.location.highResolutionGridSizeInMeters != null && raw.location.highResolutionGridSizeInMeters.length != 0) {
        raw.location.highResolutionCoordinateUncertaintyInMeters = "%.1f".format(raw.location.highResolutionGridSizeInMeters.toInt / Math.sqrt(2.0))
      }
    }

  }

  /**
    * Apply NBN-default (as defined by NBN_BLUR_GRID_SIZE) blurring to $ fields.
    * @param raw
    * @param assertions
    * Note that some spatial processing has to occur to determine if coordinate are simply centroids or are independent
    * values
    */
  private def blur(raw: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {

    var crudeGridOrCoordsOnly = true

    if (raw.location.highResolutionGridReference != null && raw.location.highResolutionGridReference != "") {
      try {
        val gridRefs = getGridRefAsResolutions(raw.location.highResolutionGridReference)
        val blurredGrid = gridRefs.getOrElse("grid_ref_" + NBN_BLUR_GRID_SIZE.toString, "")
        if (blurredGrid != "") {
          crudeGridOrCoordsOnly = false
          raw.location.gridReference = blurredGrid
          //set lat/long to centroid of new grid
          if (raw.location.decimalLongitude != null && raw.location.decimalLongitude != null) {
            val grid = gridReferenceToEastingNorthing(blurredGrid)
            grid match {
              case Some(gr) => {
                val reposition = if (!gr.gridSize.isEmpty && gr.gridSize.get > 0) {
                  gr.gridSize.get / 2
                } else {
                  0
                }

                val coordsCentroid = GISUtil.reprojectCoordinatesToWGS84(gr.easting + reposition, gr.northing + reposition, gr.datum, 5)
                val (gridCentroidLatitude, gridCentroidLongitude) = coordsCentroid.get
                raw.location.decimalLongitude = gridCentroidLongitude
                raw.location.decimalLatitude = gridCentroidLatitude
                raw.location.coordinateUncertaintyInMeters = gridToCoordinateUncertaintyString(NBN_BLUR_GRID_SIZE)
                raw.location.gridSizeInMeters = NBN_BLUR_GRID_SIZE.toString
              }
            }
          }
        } else {
          //grid is already cruder than 1km so leave as-is, but might need to round coordinates if they are not centroid
        }
      } catch {
        case e: Exception => {
          logger.error("Problem converting high resolution grid reference " + raw.location.highResolutionGridReference + " to NBN blurred resolution", e)
          raw.location.gridReference = null
        }
      }
    }

    if (crudeGridOrCoordsOnly) {
      val gisPointOption = locProcessor.processLatLong(
        raw.location.highResolutionDecimalLatitude,
        raw.location.highResolutionDecimalLongitude,
        raw.location.geodeticDatum, // if one is supplied for non high res coords then assume it's valid for high res too, and use it
        null, // raw.location.verbatimLatitude,
        null, // raw.location.verbatimLongitude,
        null, // raw.location.verbatimSRS,
        null, // raw.location.easting,
        null, // raw.location.northing,
        null, // raw.location.zone,
        raw.location.gridReference,
        assertions)

      gisPointOption match {
        case Some(gisPoint) => {
          val decimalPlacesToRoundTo = 2
          val lat: Float = gisPoint.latitude.toFloat
          val lon: Float = gisPoint.longitude.toFloat
          var isCentroid = false

          if (raw.location.gridReference != null && raw.location.gridReference.length > 0) {
            isCentroid = GridUtil.isCentroid(raw.location.decimalLongitude.toDouble, raw.location.decimalLatitude.toDouble, raw.location.gridReference)
          } else {
            //calc temp grid if gridSize is provided
            if (raw.location.gridSizeInMeters != null && raw.location.gridSizeInMeters.length > 0) {
              val grid = getGridFromLatLonAndGridSize(lat, lon, raw.location.gridSizeInMeters, raw.location.stateProvince, raw.location.geodeticDatum)
              isCentroid = GridUtil.isCentroid(raw.location.decimalLongitude.toDouble, raw.location.decimalLatitude.toDouble, grid.get)
            }
          }
          if (!isCentroid) {
            //round if not centroid of crude grid, or no grid specified
            var coordUncert:Float = raw.location.coordinateUncertaintyInMeters.toFloatWithOption.getOrElse(0)
            if ((raw.location.coordinateUncertaintyInMeters == null || raw.location.coordinateUncertaintyInMeters == "") &&
              (raw.location.gridSizeInMeters != null && raw.location.gridSizeInMeters.length > 0)) {
              coordUncert = (raw.location.gridSizeInMeters.toFloat / math.sqrt(2.0)).toFloat
              //TODO: assertion if coordUncert and gridSize both provided and do not agree?
            }
            if (coordUncert < gridToCoordinateUncertaintyString(NBN_BLUR_GRID_SIZE).toFloat) {
              //and not a low precision record
              raw.location.decimalLatitude = Precision.round(lat, decimalPlacesToRoundTo).toString
              raw.location.decimalLongitude = Precision.round(lon, decimalPlacesToRoundTo).toString
              raw.location.coordinateUncertaintyInMeters = gridToCoordinateUncertaintyString(NBN_BLUR_GRID_SIZE)
              raw.location.gridSizeInMeters = NBN_BLUR_GRID_SIZE.toString
            }
          }
        }
        case None => {
          raw.location.decimalLatitude = null
          raw.location.decimalLongitude = null
          raw.location.coordinateUncertaintyInMeters = null
          raw.location.gridSizeInMeters = null
        }
      }
    }
    raw.location.locality = null
    raw.event.eventID = null

  }

  /**
    * Replace $ fields with originalBlurredValues
    * @param raw
    * @param assertions
    */
  private def unpackOriginalBlurredValues(raw: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    //TODO: this could be a duplication of what will happen when the originalSensitiveValues are unpacked (and by extension for highres records, the originalBlurredValues)
    raw.location.decimalLatitude = raw.occurrence.originalBlurredValues.getOrElse("decimalLatitude", null)
    raw.location.decimalLongitude = raw.occurrence.originalBlurredValues.getOrElse("decimalLongitude", null)
    raw.location.gridReference = raw.occurrence.originalBlurredValues.getOrElse("gridReference", null)
    raw.location.gridSizeInMeters = raw.occurrence.originalBlurredValues.getOrElse("gridSizeInMeters", null)
    raw.location.coordinateUncertaintyInMeters = raw.occurrence.originalBlurredValues.getOrElse("coordinateUncertaintyInMeters", null)
    raw.location.locality = raw.occurrence.originalBlurredValues.getOrElse("locality", null)
    raw.event.eventID = raw.occurrence.originalBlurredValues.getOrElse("eventID", null)
  }

  /**
    * Set originalBlurredValues field from $ fields
    * @param raw
    * @param assertions
    */
  private def packOriginalBlurredValues(raw: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    var originalBlurredValues = scala.collection.mutable.Map[String, String]()
    if (raw.location.decimalLatitude != null && raw.location.decimalLatitude.length > 0) {
      originalBlurredValues += ("decimalLatitude" -> raw.location.decimalLatitude)
    }
    if (raw.location.decimalLongitude != null && raw.location.decimalLongitude.length > 0) {
      originalBlurredValues += ("decimalLongitude" -> raw.location.decimalLongitude)
    }
    if (raw.location.coordinateUncertaintyInMeters != null && raw.location.coordinateUncertaintyInMeters.length > 0) {
      originalBlurredValues += ("coordinateUncertaintyInMeters" -> raw.location.coordinateUncertaintyInMeters)
    }
    if (raw.location.gridReference != null && raw.location.gridReference.length > 0) {
      originalBlurredValues += ("gridReference" -> raw.location.gridReference)
    }
    if (raw.location.gridSizeInMeters != null && raw.location.gridSizeInMeters.length > 0) {
      originalBlurredValues += ("gridSizeInMeters" -> raw.location.gridSizeInMeters)
    }
    raw.occurrence.originalBlurredValues = originalBlurredValues.toMap
  }

  /**
    * set originalSensitiveValues field from highresolution$ fields
    * @param raw
    * @param assertions
    */
  private def packOriginalHighResolutionValues(raw: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    //TODO: this could be a duplication of what will happen when the originalSensitiveValues are packed
    var originalSensitiveValues = scala.collection.mutable.Map[String, String]()
    if (raw.location.highResolutionDecimalLatitude != null && raw.location.highResolutionDecimalLatitude.length > 0) {
      originalSensitiveValues += ("decimalLatitude" -> raw.location.highResolutionDecimalLatitude)
    }
    if (raw.location.highResolutionDecimalLongitude != null && raw.location.highResolutionDecimalLongitude.length > 0) {
      originalSensitiveValues += ("decimalLongitude" -> raw.location.highResolutionDecimalLongitude)
    }
    if (raw.location.highResolutionCoordinateUncertaintyInMeters != null && raw.location.highResolutionCoordinateUncertaintyInMeters.length > 0) {
      originalSensitiveValues += ("coordinateUncertaintyInMeters" -> raw.location.highResolutionCoordinateUncertaintyInMeters)
    }
    if (raw.location.highResolutionGridReference != null && raw.location.highResolutionGridReference.length > 0) {
      originalSensitiveValues += ("gridReference" -> raw.location.highResolutionGridReference)
    }
    if (raw.location.highResolutionGridSizeInMeters != null && raw.location.highResolutionGridSizeInMeters.length > 0) {
      originalSensitiveValues += ("gridSizeInMeters" -> raw.location.highResolutionGridSizeInMeters)
    }
    if (raw.location.highResolutionLocality != null && raw.location.highResolutionLocality.length > 0) {
      originalSensitiveValues += ("locality" -> raw.location.highResolutionLocality)
    }
    if (raw.event.highResolutionEventID != null && raw.event.highResolutionEventID.length > 0) {
      originalSensitiveValues += ("eventID" -> raw.event.highResolutionEventID)
    }
    raw.occurrence.originalSensitiveValues = originalSensitiveValues.toMap
  }

  /**
    * Get list of fields updated during pre-processing to persist
    * @param raw
    * @return
    */
  private def getFieldsToUpdateMap(raw: FullRecord): Map[String, String] = {
    var fieldsToUpdate = Map[String, String]()
    fieldsToUpdate += ("decimalLatitude" -> raw.location.decimalLatitude)
    fieldsToUpdate += ("decimalLongitude" -> raw.location.decimalLongitude)
    fieldsToUpdate += ("coordinateUncertaintyInMeters" -> raw.location.coordinateUncertaintyInMeters)
    fieldsToUpdate += ("gridReference" -> raw.location.gridReference)
    fieldsToUpdate += ("gridSizeInMeters" -> raw.location.gridSizeInMeters)
    fieldsToUpdate += ("locality" -> raw.location.locality)
    fieldsToUpdate += ("eventID" -> raw.event.eventID)

    //note, don't save changes to high-res fields since on re-processing need the state of these to be as-originally-loaded

    if (raw.occurrence.originalBlurredValues != null) {
      val originalBlurredValuesJSON = Json.toJSON(raw.occurrence.originalBlurredValues)
      fieldsToUpdate += ("originalBlurredValues" -> originalBlurredValuesJSON)
    } else {
      fieldsToUpdate += ("originalBlurredValues" -> null)
    }
    if (raw.occurrence.originalSensitiveValues != null) {
      val originalSensitiveValuesJSON = Json.toJSON(raw.occurrence.originalSensitiveValues)
      fieldsToUpdate += ("originalSensitiveValues" -> originalSensitiveValuesJSON)
    } else {
      fieldsToUpdate += ("originalSensitiveValues" -> null)
    }

    fieldsToUpdate
  }

  /**
    * Helper function to calculate coordinate uncertainty for a given grid size
    * @param gridSize
    * @return
    */
  def gridToCoordinateUncertaintyString(gridSize: Integer):String = {
    "%.1f".format(gridSize / math.sqrt(2.0))
  }

  /**
    * As per other processing steps 'skip' functions
    * @param guid
    * @param raw
    * @param processed
    * @param lastProcessed
    * @return
    */
  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]
    if (lastProcessed.isDefined) {
      assertions ++= lastProcessed.get.findAssertions(Array(LOCATION_NOT_SUPPLIED.code,
        COUNTRY_INFERRED_FROM_COORDINATES.code, COORDINATES_CENTRE_OF_STATEPROVINCE.code,
        COORDINATES_CENTRE_OF_COUNTRY.code, MIN_MAX_DEPTH_REVERSED.code, MIN_MAX_ALTITUDE_REVERSED.code,
        ALTITUDE_OUT_OF_RANGE.code, ALTITUDE_NON_NUMERIC.code, ALTITUDE_IN_FEET.code, DEPTH_OUT_OF_RANGE.code,
        DEPTH_NON_NUMERIC.code, DEPTH_IN_FEET.code, DECIMAL_COORDINATES_NOT_SUPPLIED.code,
        DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.code, GEODETIC_DATUM_ASSUMED_WGS84.code,
        UNRECOGNIZED_GEODETIC_DATUM.code, DECIMAL_LAT_LONG_CONVERSION_FAILED.code, DECIMAL_LAT_LONG_CONVERTED.code,
        DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED.code, DECIMAL_LAT_LONG_CALCULATED_FROM_VERBATIM.code,
        UNCERTAINTY_RANGE_MISMATCH.code, UNCERTAINTY_IN_PRECISION.code, MISSING_COORDINATEPRECISION.code,
        PRECISION_RANGE_MISMATCH.code, COORDINATE_PRECISION_MISMATCH.code, PRECISION_RANGE_MISMATCH.code,
        UNCERTAINTY_NOT_SPECIFIED.code, COORDINATE_HABITAT_MISMATCH.code, STATE_COORDINATE_MISMATCH.code,
        MISSING_GEODETICDATUM.code, MISSING_GEOREFERNCEDBY.code, MISSING_GEOREFERENCEPROTOCOL.code,
        MISSING_GEOREFERENCESOURCES.code, MISSING_GEOREFERENCEVERIFICATIONSTATUS.code, MISSING_GEOREFERENCE_DATE.code,
        INVERTED_COORDINATES.code, COORDINATES_OUT_OF_RANGE.code, ZERO_COORDINATES.code, ZERO_LATITUDE_COORDINATES.code,
        ZERO_LONGITUDE_COORDINATES.code, UNKNOWN_COUNTRY_NAME.code, NEGATED_LATITUDE.code, NEGATED_LONGITUDE.code,
        COUNTRY_COORDINATE_MISMATCH.code, COORDINATES_NOT_CENTRE_OF_GRID.code))

      //update the details from lastProcessed
      processed.location = lastProcessed.get.location
    }

    assertions.toArray
  }

  def getName = FullRecordMapper.geospatialQa
}

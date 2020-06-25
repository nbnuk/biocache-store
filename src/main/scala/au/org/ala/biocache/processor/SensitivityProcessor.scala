package au.org.ala.biocache.processor

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.{LocationDAO, SensitivityDAO, SpatialLayerDAO}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, QualityAssertion, Versions}
import au.org.ala.biocache.util.{GridUtil, Json}
import au.org.ala.biocache.vocab.StateProvinces
import au.org.ala.sds.SensitiveDataService
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Performs sensitive data processing on the record.
  * Where required this will reduce the quality of location and event information.
  */
class SensitivityProcessor extends Processor {

  val logger = LoggerFactory.getLogger("SensitivityProcessor")

  import JavaConversions._

  def getName = "sensitive"

  /**
    * Process the supplied record.
    *
    * @param guid
    * @param raw
    * @param processed
    * @param lastProcessed
    * @return
    */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None):
  Array[QualityAssertion] = {

    // if SDS disabled, do nothing
    if (!Config.sdsEnabled) {
      logger.debug("[SDS Debug] SDS disabled")
      return Array()
    } else {
      logger.debug("[SDS Debug] SDS enabled")
    }

    val exact = getExactSciName(raw)

    val isSensitive = SensitivityDAO.isSensitive(exact, processed.classification.taxonConceptID)
    logger.debug("[SDS Debug] Name is associated with sensitive species: " + exact)

    //is the name recognised as sensitive?
    if (!isSensitive) {
      logger.debug("[SDS Debug] Name is NOT associated with sensitive species: " + exact)
      return Array()
    } else {
      logger.debug("[SDS Debug] Name is associated with sensitive species: " + exact)
    }

    //needs to be performed for all records whether or not they are in Australia
    //get a map representation of the raw record for sdsFlag fields...
    val rawMap = new java.util.HashMap[String, String]()
    rawMap.putAll(raw.getRawFields())
    if (rawMap.isEmpty) {
      //populate rawMap if raw.rawFields is empty
      raw.objectArray.foreach { poso =>
        val map = FullRecordMapper.mapObjectToProperties(poso, Versions.RAW)
        rawMap ++= map
      }
    }

    //use the processed versions of the coordinates for the sensitivity check if raw not available
    //this would be the case when coordinates have been derived from easting/northings or grid references
    if (!raw.location.hasCoordinates && processed.location.hasCoordinates) {
      rawMap.put("decimalLatitude", processed.location.decimalLatitude)
      rawMap.put("decimalLongitude", processed.location.decimalLongitude)
      rawMap.put("coordinatePrecision", processed.location.coordinatePrecision)
      rawMap.put("coordinateUncertaintyInMeters", processed.location.coordinateUncertaintyInMeters)
    }



    //temporary
    /*
    var sensitiveGridDifferentFromBlurred = false
    var originalRawMapGridReference = ""
    if(raw.occurrence.originalSensitiveValues != null) {
      if (raw.occurrence.originalSensitiveValues.contains("gridReference") &&
        rawMap.contains("gridreference")) {
        if (rawMap("gridreference") != raw.occurrence.originalSensitiveValues("gridReference")) {
          sensitiveGridDifferentFromBlurred = true
          originalRawMapGridReference = rawMap("gridreference")
        }
      }
    }
    */

    //does the object have some original sensitive values
    //these should override the processed versions
    //NBN: but, won't these overwrite updated values in a record that has been reloaded?

    if(raw.occurrence.originalSensitiveValues != null){
      //update the raw object.....
      raw.occurrence.originalSensitiveValues.foreach {
        case (key, value) => {
          raw.setProperty(key, value)
          rawMap.put(key.toLowerCase, value) //to lower case because of inconsistency (sds1.4.4 sets these with camelcasing)
        }
      }
    }

    //hopefully one-off fix for incorrectly processed sensitive records with grid instead of coordinates: the generalised lat/long values were
    //stored in originalsensitivevalues instead of the grid centroid, which might be more precise
    //e.g. {"coordinateUncertaintyInMeters_p":"100.0","decimalLongitude":"-4.6","decimalLatitude":"54.1","gridReference":"SC303697"}
    /*
    if(raw.occurrence.originalSensitiveValues != null) {
      if (raw.occurrence.originalSensitiveValues.contains("gridReference") &&
        Config.gridRefIndexingEnabled && raw.location.gridReference != null &&
        raw.occurrence.originalSensitiveValues.contains("decimalLatitude") &&
        raw.occurrence.originalSensitiveValues.contains("decimalLongitude")) {
        if (sensitiveGridDifferentFromBlurred) {
          //if sensitive coords are centroid of non-sensitive (i.e. blurred) grid, then recalc using sensitive grid
          //assumes that record did not have both grid and coords supplied
          //below doesn't work because lat/longs rounded off before storage baed on coord uncertainty, so differ from the actual centroid
          /* val checkPnt = GridUtil.processGridReference(originalRawMapGridReference)
          if ((checkPnt.get.latitude.toDouble - raw.occurrence.originalSensitiveValues("decimalLatitude").toDouble).abs < 0.001 &&
            (checkPnt.get.longitude.toDouble - raw.occurrence.originalSensitiveValues("decimalLongitude").toDouble).abs < 0.001) { */
          if (rawMap("datageneralizations_p").contains("already generalised")) {
            val newPnt = GridUtil.processGridReference(raw.occurrence.originalSensitiveValues("gridReference"))
            raw.occurrence.originalSensitiveValues -= "decimalLongitude"
            raw.occurrence.originalSensitiveValues -= "decimalLatitude"


            raw.location.decimalLongitude = newPnt.get.longitude
            raw.location.decimalLatitude = newPnt.get.latitude
            raw.location.gridReference = raw.occurrence.originalSensitiveValues("gridReference")
            rawMap -= "decimallongitude"
            rawMap -= "decimallatitude"
            rawMap -= "gridreference"
            rawMap.put("decimallongitude", newPnt.get.longitude)
            rawMap.put("decimallatitude", newPnt.get.latitude)
            rawMap.put("gridreference", raw.occurrence.originalSensitiveValues("gridReference"))

            //if (rawMap.contains("decimallatitude")) rawMap -= "decimallatitude"
            //if (rawMap.contains("decimallongitude")) rawMap -= "decimallongitude"
          }
        }
      }
    }
    */

    if (processed.location.hasCoordinates) {

      //do a dynamic lookup for the layers required for the SDS
      val layerIntersect = SpatialLayerDAO.intersect(
        processed.location.decimalLongitude.toDouble,
        processed.location.decimalLatitude.toDouble)

      SpatialLayerDAO.sdsLayerList.foreach { key =>
        rawMap.put(key, layerIntersect.getOrElse(key, "n/a"))
      }

      val intersectStateProvince = layerIntersect.getOrElse(Config.stateProvinceLayerID, "")

      //reset
      if (StringUtils.isBlank(intersectStateProvince)) {
        val stringMatchState = StateProvinces.matchTerm(raw.location.stateProvince)
        if (!stringMatchState.isEmpty) {
          rawMap.put("stateProvince", stringMatchState.get.canonical)
        }
      } else {
        rawMap.put("stateProvince", intersectStateProvince)
      }

      logger.debug("Intersections: " + rawMap.toMap.mkString(";"))
    } else {
      logger.debug("No coordinates - no intersections")
    }

    //this flag stops on the fly sampling being performed by SDS
    rawMap.put(SensitiveDataService.SAMPLED_VALUES_PROVIDED, "true")

    //put the processed event date components in to allow for correct date applications of the rules
    if (processed.event.day != null)
      rawMap("day") = processed.event.day
    if (processed.event.month != null)
      rawMap("month") = processed.event.month
    if (processed.event.year != null)
      rawMap("year") = processed.event.year
    if (processed.event.endDay != null)
      rawMap("endDay") = processed.event.endDay
    if (processed.event.endMonth != null)
      rawMap("endMonth") = processed.event.endMonth
    if (processed.event.endYear != null)
      rawMap("endYear") = processed.event.endYear

    if (logger.isDebugEnabled()) {
      logger.debug("Testing with the following properties: " + rawMap + ", and Taxon Concept ID :" + processed.classification.taxonConceptID)
    }

    //SDS check - now get the ValidationOutcome from the Sensitive Data Service
    //sds1.4.4 expects rawMap to have camelcase keys, not all lowercase, so it fails to find e.g. decimalLatitude
    //TODO fix sds. This is a hacky workaround
    if (rawMap.contains("decimallatitude")) rawMap("decimalLatitude") = rawMap("decimallatitude")
    if (rawMap.contains("decimallongitude")) rawMap("decimalLongitude") = rawMap("decimallongitude")
    if (rawMap.contains("dataresourceuid")) rawMap("dataResourceUid") = rawMap("dataresourceuid")


    val outcome = SensitivityDAO.getSDS.testMapDetails(Config.sdsFinder, rawMap, exact, processed.classification.taxonConceptID)
    //TODO: do we need to set any rawMap lowercase fields from the camelcase entries in outcome? eg. locationRemarks etc.
    //TODO: not sure. Need test record.

    logger.debug("SDS outcome: " + outcome)

    /************** SDS check end ************/

    if (outcome != null && outcome.isValid && outcome.isSensitive) {

      logger.debug("Taxon identified as sensitive.....")
      if (outcome.getResult != null) {

        //convert it to a string string map
        val rawPropertiesToUpdate = outcome.getResult.collect {
          case (key, value) if value != null => if (key == "originalSensitiveValues") {
            val originalSensitiveValues = value.asInstanceOf[java.util.HashMap[String, String]]
            // add the original "processed" coordinate uncertainty to the sensitive values so that it
            // can be available if necessary
            if (StringUtils.isNotBlank(processed.location.coordinateUncertaintyInMeters)) {
              originalSensitiveValues.put("coordinateUncertaintyInMeters" + Config.persistenceManager.fieldDelimiter + "p",
                processed.location.coordinateUncertaintyInMeters)
            }
            if (StringUtils.isNotBlank(raw.location.gridReference)) {
              originalSensitiveValues.put("gridReference", raw.location.gridReference)
            }

            if (StringUtils.isNotBlank(processed.location.gridReference)) {
              //store processed high-resolution grid reference
              originalSensitiveValues.put("gridReference" + Config.persistenceManager.fieldDelimiter + "p", processed.location.gridReference)
            }

            originalSensitiveValues.put("eventID", raw.event.eventID)
            if (Config.sensitiveDateDay) {
              originalSensitiveValues.put("eventDate", raw.event.eventDate)
              originalSensitiveValues.put("eventDateEnd", raw.event.eventDateEnd)
              originalSensitiveValues.put("eventTime", raw.event.eventTime)
              originalSensitiveValues.put("day", raw.event.day)
              originalSensitiveValues.put("month", raw.event.month)
              originalSensitiveValues.put("endDay", raw.event.endDay)
              originalSensitiveValues.put("endMonth", raw.event.endMonth)
              originalSensitiveValues.put("verbatimEventDate", raw.event.verbatimEventDate)
            }

            //remove all the el/cl's from the original sensitive values
            SpatialLayerDAO.sdsLayerList.foreach { key => originalSensitiveValues.remove(key) }
            (key -> Json.toJSON(originalSensitiveValues))
          } else {
            (key -> value.toString)
          }
        }

        val currentUncertainty = if (StringUtils.isNotEmpty(processed.location.coordinateUncertaintyInMeters)) {
          java.lang.Float.parseFloat(processed.location.coordinateUncertaintyInMeters)
        } else {
          0
        }
        //take away the values that need to be added to the processed record NOT the raw record
        var uncertainty = rawPropertiesToUpdate.get("generalisationInMetres")
        //not sure about uncertainty vs. generalisationToApplyInMetres - should we treat uncertainty in a grid way or as a linear distance?
        if (uncertainty.isDefined) {
          if (uncertainty.get != null && uncertainty.get != "") {
            //rewrite as grid dist centre to point
            val cornerDistFromCentre = java.lang.Integer.parseInt(uncertainty.get).toDouble / math.sqrt(2.0) //math.sqrt(2.0 * math.pow(sideDistFromCentre,2))
            uncertainty = Some("%.1f".format(cornerDistFromCentre))
          }
        }
        val generalisationToApplyInMetresGrid = rawPropertiesToUpdate.get("generalisationToApplyInMetres")
        var generalisationToApplyInMetres = generalisationToApplyInMetresGrid
        if (generalisationToApplyInMetresGrid.isDefined) {
          if (generalisationToApplyInMetresGrid.get != null && generalisationToApplyInMetresGrid.get != "") {
            //rewrite as grid dist centre to point
            val cornerDistFromCentre = java.lang.Integer.parseInt(generalisationToApplyInMetresGrid.get).toDouble / math.sqrt(2.0) //math.sqrt(2.0 * math.pow(sideDistFromCentre,2))
            generalisationToApplyInMetres = Some("%.1f".format(cornerDistFromCentre))
          }
        }
        var centroidAlreadyGeneralised = false
        if (!uncertainty.isEmpty) {
          //if centroid provided then don't generalise further if existing grid is bigger than sensitive grid
          if (currentUncertainty >= java.lang.Float.parseFloat(uncertainty.get.toString)) {
            val isCentroid =
              if (processed.location.gridReference != null && processed.location.gridReference.length > 0) {
                GridUtil.isCentroid(rawMap("decimalLongitude").toDouble, rawMap("decimalLatitude").toDouble, processed.location.gridReference)
              } else if (raw.location.gridReference != null && raw.location.gridReference.length > 0) {
                GridUtil.isCentroid(rawMap("decimalLongitude").toDouble, rawMap("decimalLatitude").toDouble, raw.location.gridReference)
              } else {
                false
              }
            if (isCentroid) {
              centroidAlreadyGeneralised = true
              rawPropertiesToUpdate -= "decimalLatitude"
              rawPropertiesToUpdate -= "decimalLongitude"
            }
          }
        }
        if (!centroidAlreadyGeneralised) {
          if (!uncertainty.isEmpty) {
            //we know that we have sensitised, add the uncertainty to the currently processed uncertainty
            val newUncertainty = currentUncertainty + java.lang.Float.parseFloat(uncertainty.get.toString)
            processed.location.coordinateUncertaintyInMeters = "%.1f".format(newUncertainty)
          }

          processed.location.decimalLatitude = rawPropertiesToUpdate.getOrElse("decimalLatitude", "")
          processed.location.decimalLongitude = rawPropertiesToUpdate.getOrElse("decimalLongitude", "")
          processed.location.northing = ""
          processed.location.easting = ""
          processed.location.bbox = ""
          rawPropertiesToUpdate -= "generalisationInMetres"


          //remove other GIS references
          if (Config.gridRefIndexingEnabled && raw.location.gridReference != null) {

            if (generalisationToApplyInMetres.isDefined) {
              //reduce the quality of the grid reference
              if (generalisationToApplyInMetres.get == null || generalisationToApplyInMetres.get == "") {
                rawPropertiesToUpdate.put("gridReference", "")
                processed.setProperty("gridSizeInMeters", "")
              } else {
                if (currentUncertainty >= java.lang.Float.parseFloat(generalisationToApplyInMetres.get)) {
                  //raw coordinate uncertainty is already cruder than the SDS-derived generalisation
                  processed.location.coordinateUncertaintyInMeters = "%.1f".format(currentUncertainty)
                  processed.location.decimalLatitude = rawMap("decimalLatitude")
                  processed.location.decimalLongitude = rawMap("decimalLongitude")
                  rawPropertiesToUpdate("decimalLatitude") = rawMap("decimalLatitude")
                  rawPropertiesToUpdate("decimalLongitude") = rawMap("decimalLongitude")
                  rawPropertiesToUpdate("dataGeneralizations") = rawPropertiesToUpdate("dataGeneralizations").replace(" generalised", " is already generalised")
                } else {
                  processed.location.coordinateUncertaintyInMeters = "%.1f".format(currentUncertainty.toDouble + java.lang.Float.parseFloat(generalisationToApplyInMetres.get))
                }

                val generalisedRef = GridUtil.convertReferenceToResolution(raw.location.gridReference, generalisationToApplyInMetresGrid.get)
                if (generalisedRef.isDefined) {
                  rawPropertiesToUpdate.put("gridReference", generalisedRef.get)
                  processed.setProperty("gridSizeInMeters", GridUtil.getGridSizeInMeters(generalisedRef.get).getOrElse("").toString())
                } else {
                  rawPropertiesToUpdate.put("gridReference", "")
                  processed.setProperty("gridSizeInMeters", "")
                }
              }
            } else {
              rawPropertiesToUpdate.put("gridReference", "")
              processed.setProperty("gridSizeInMeters", "")
            }
          }

          //if grid reference was derived from coordinates
          if (processed.location.gridReference != null) {
            if (generalisationToApplyInMetres.isDefined) {
              //reduce the quality of the grid reference
              if (generalisationToApplyInMetres.get == null || generalisationToApplyInMetres.get == "") {
                processed.setProperty("gridReference", "")
                processed.setProperty("gridSizeInMeters", "")
              } else {
                val generalisedRef = GridUtil.convertReferenceToResolution(processed.location.gridReference, generalisationToApplyInMetresGrid.get)
                if (generalisedRef.isDefined) {
                  processed.setProperty("gridReference", generalisedRef.get)
                  processed.setProperty("gridSizeInMeters", GridUtil.getGridSizeInMeters(generalisedRef.get).getOrElse("").toString())
                } else {
                  processed.setProperty("gridReference", "")
                  processed.setProperty("gridSizeInMeters", "")
                }
              }
            } else {
              processed.setProperty("gridReference", "")
              processed.setProperty("gridSizeInMeters", "")
            }
          }
        }

        // add a guard here as we may have already updated this field when building WKT
        if ((processed.occurrence.informationWithheld == null) || (processed.occurrence.informationWithheld == "")) {
          processed.occurrence.informationWithheld = rawPropertiesToUpdate.getOrElse("informationWithheld", "")
          rawPropertiesToUpdate -= "informationWithheld"
        }

        processed.occurrence.dataGeneralizations = rawPropertiesToUpdate.getOrElse("dataGeneralizations", "")
        rawPropertiesToUpdate -= "dataGeneralizations"

        //remove the day from the values if present
        if (Config.sensitiveDateDay) {
          raw.event.day = ""
          raw.event.eventDate = ""
          raw.event.endDay = ""
          raw.event.eventDateEnd = ""
          raw.event.eventTime = ""
          raw.event.verbatimEventDate = ""
        }
        raw.location.easting = ""
        raw.location.northing = ""
        raw.event.eventID = ""

        if (Config.sensitiveDateDay) {
          processed.event.day = ""
          if (processed.event.endDay != null) {
            processed.event.endDay = ""
          }
          processed.event.eventDate = ""
          if (processed.event.eventDateEnd != null) {
            processed.event.eventDateEnd = ""
          }
          if (processed.event.eventTime != null) {
            processed.event.eventTime = ""
          }
        }

        //remove this field values
        if (Config.sensitiveDateDay) {
          rawPropertiesToUpdate.put("day", "")
          rawPropertiesToUpdate.put("endDay", "")
          rawPropertiesToUpdate.put("eventDate", "")
          rawPropertiesToUpdate.put("eventDateEnd", "")
          rawPropertiesToUpdate.put("eventTime", "")
          rawPropertiesToUpdate.put("verbatimEventDate", "")
        }
        rawPropertiesToUpdate.put("northing", "")
        rawPropertiesToUpdate.put("easting", "")
        rawPropertiesToUpdate.put("eventID", "")

        if (!Config.sensitiveDateDay) {
          if (raw.event.eventDate != null) {
            rawPropertiesToUpdate.put("eventDate", raw.event.eventDate)
          } else if (processed.event.eventDate != null) {
            rawPropertiesToUpdate.put("eventDate", processed.event.eventDate)
          }
          if (raw.event.eventDateEnd != null) {
            rawPropertiesToUpdate.put("eventDateEnd", raw.event.eventDateEnd)
          } else if (processed.event.eventDateEnd != null) {
            rawPropertiesToUpdate.put("eventDateEnd", processed.event.eventDateEnd)
          }
          if (raw.event.eventTime != null) {
            rawPropertiesToUpdate.put("eventTime", raw.event.eventTime)
          } else if (processed.event.eventTime != null) {
            rawPropertiesToUpdate.put("eventTime", processed.event.eventTime)
          }
          if (raw.event.verbatimEventDate != null) {
            rawPropertiesToUpdate.put("verbatimEventDate", raw.event.verbatimEventDate)
          } else if (processed.event.verbatimEventDate != null) {
            rawPropertiesToUpdate.put("verbatimEventDate", processed.event.verbatimEventDate)
          }
        }
        //update the object for downstream processing
        rawPropertiesToUpdate.foreach { case (key, value) => raw.setProperty(key, value) }

        //update the raw record, removing properties where necessary
        if (StringUtils.isNotBlank(raw.rowKey)) {
          Config.persistenceManager.put(raw.rowKey, "occ", rawPropertiesToUpdate.toMap, false, false)

          try {
            if (StringUtils.isNotBlank(processed.location.decimalLatitude) &&
              StringUtils.isNotBlank(processed.location.decimalLongitude)) {
              //store the generalised coordinates for down stream sampling
              LocationDAO.storePointForSampling(processed.location.decimalLatitude, processed.location.decimalLongitude)
            }
          } catch {
            case e: Exception => {
              logger.error("Error storing point for sampling for SDS record: " + raw.rowKey + " " + processed.rowKey, e)
            }
          }
        }

      } else if (!outcome.isLoadable() && Config.obeySDSIsLoadable) {
        logger.debug("SDS isLoadable status is currently not being used. Would apply to: " + processed.rowKey)
      }

      if (outcome.getReport().getMessages() != null) {
        var infoMessage = ""
        outcome.getReport().getMessages().foreach(message => {
          infoMessage += message.getCategory() + "\t" + message.getMessageText() + "\n"
        })
        processed.occurrence.informationWithheld = infoMessage
      }

      // recalculate footprint and informationWithheld annotation using generalised grid
      if (raw.location.gridReference != null) {
        raw.location.gridReferenceWKT = GridUtil.getGridWKT(raw.location.gridReference)
        processed.location.gridReferenceWKT = raw.location.gridReferenceWKT
        //TODO: recalc raw gridSizeInMeters if this is allowed to be provided?
        processed.occurrence.informationWithheld = GridUtil.getGridAsTextWithAnnotation( raw.location.gridReference );
        // note, we don't overwrite raw.occurrence.informationWithheld, as we might prefer that untouched
      }

    } else {
      //Species is NOT sensitive
      //if the raw record has originalSensitive values we need to re-initialise the value
      if (StringUtils.isNotBlank(raw.rowKey) &&
        raw.occurrence.originalSensitiveValues != null &&
        !raw.occurrence.originalSensitiveValues.isEmpty) {
        Config.persistenceManager.put(raw.rowKey, "occ", raw.occurrence.originalSensitiveValues + ("originalSensitiveValues" -> ""), false, false)
      }
    }

    Array()
  }

  /**
   * Retrieve an scientific name to use for SDS processing.
   *
   * @param raw
   * @return
   */
  private def getExactSciName(raw: FullRecord) : String = {
    if (raw.classification.scientificName != null)
      raw.classification.scientificName
    else if (raw.classification.subspecies != null)
      raw.classification.subspecies
    else if (raw.classification.species != null)
      raw.classification.species
    else if (raw.classification.genus != null) {
      if (raw.classification.specificEpithet != null) {
        if (raw.classification.infraspecificEpithet != null)
          raw.classification.genus + " " + raw.classification.specificEpithet + " " + raw.classification.infraspecificEpithet
        else
          raw.classification.genus + " " + raw.classification.specificEpithet
      } else {
        raw.classification.genus
      }
    }
    else if (raw.classification.vernacularName != null) // handle the case where only a common name is provided.
      raw.classification.vernacularName
    else //return the name default name string which will be null
      raw.classification.scientificName
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    val assertions = new ArrayBuffer[QualityAssertion]

    //get the data resource information to check if it has mapped collections
    if (lastProcessed.isDefined) {
      //no assertions
      //assertions ++= lastProcessed.get.findAssertions(Array())

      //update the details from lastProcessed
      processed.location.coordinateUncertaintyInMeters = lastProcessed.get.location.coordinateUncertaintyInMeters
      processed.location.decimalLatitude = lastProcessed.get.location.decimalLatitude
      processed.location.decimalLongitude = lastProcessed.get.location.decimalLatitude
      processed.location.northing = lastProcessed.get.location.northing
      processed.location.easting = lastProcessed.get.location.easting
      processed.location.bbox = lastProcessed.get.location.bbox
      processed.occurrence.informationWithheld = lastProcessed.get.occurrence.informationWithheld
      processed.occurrence.dataGeneralizations = lastProcessed.get.occurrence.dataGeneralizations
      processed.event.day = lastProcessed.get.event.day //was eventDateEnd ???
      processed.event.endDay = lastProcessed.get.event.endDay
      processed.event.eventDate = lastProcessed.get.event.eventDate //was eventDateEnd ???
      processed.event.eventDateEnd = lastProcessed.get.event.eventDateEnd
    }

    assertions.toArray
  }
}

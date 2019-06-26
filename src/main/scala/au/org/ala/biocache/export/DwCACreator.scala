package au.org.ala.biocache.export

import java.util.zip.ZipOutputStream
import java.io.{FileOutputStream, OutputStreamWriter}
import java.util.zip.ZipEntry
import au.com.bytecode.opencsv.CSVWriter
import scala.io.Source
import org.apache.commons.io.FileUtils
import scala.util.parsing.json.JSON
import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.OptionParser
import util.matching.Regex
import au.org.ala.biocache.cmd.Tool

/**
 * Companion object for the DwCACreator class.
 */
object DwCACreator extends Tool {

  def cmd = "create-dwc"

  def desc = "Create Darwin Core Archive for a data resource"

  val logger = LoggerFactory.getLogger("DwCACreator")

  val defaultFields = List(
    "rowkey",
    "basisofrecord_p",
    "behavior",
    "catalognumber",
    "class_p",
    "collectioncode",
    "coordinateuncertaintyinmeters_p",
    "country_p",
    "datageneralizations_p",
    "day_p",
    "decimallatitude_p",
    "decimallongitude_p",
    "dynamicproperties",
    "eventdate_p",
    "eventid",
    "eventremarks",
    "family_p",
    "fieldnotes",
    "genus_p",
    "geodeticdatum_p",
    "georeferenceverificationstatus",
    "gridreferencewkt_p",
    "habitat_p",
    "highergeography",
    "identificationverificationstatus_p",
    "identifiedby",
    "individualcount",
    "informationwithheld_p",
    "institutioncode",
    "kingdom_p",
    "license_p",
    "lifestage",
    "locality",
    "locationremarks",
    "maximumdepthinmeters",
    "maximumelevationinmeters",
    "measurementmethod",
    "measurementtype",
    "measurementunit",
    "measurementvalue",
    "minimumdepthinmeters",
    "minimumelevationinmeters",
    "month_p",
    "nomenclaturalstatus_p",
    "occurrenceid",
    "occurrenceremarks",
    "occurrencestatus_p",
    "order_p",
    "organismquantity",
    "organismquantitytype",
    "organismremarks",
    "organismscope",
    "phylum_p",
    "recordedby_p",
    "recordnumber",
    "rightsholder",
    "samplesizeunit",
    "samplesizevalue",
    "samplingeffort",
    "samplingprotocol",
    "scientificname_p",
    "scientificnameauthorship_p",
    "sex",
    "stateprovince_p",
    "taxonconceptid_p",
    "taxonid",
    "taxonrank_p",
    "verbatimdepth",
    "vernacularname_p",
    "year_p"
  )


  def main(args: Array[String]): Unit = {

    var resourceUid = ""
    var directory = ""
    var threads = 4

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "The UID of the data resource to load or 'all' to generate for all",
        { v: String => resourceUid = v }
      )
      arg("directory-to-dump", "skip the download and use local file",
        { v:String => directory = v }
      )
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
    }

    if(parser.parse(args)){
      val dwcc = new DwCACreator
      try {
        var dataResource2OutputStreams = Map[String, Option[(ZipOutputStream, CSVWriter)]]()
        if (resourceUid == "" || resourceUid == "all") {
          dataResource2OutputStreams = getDataResourceUids.map { uid => (uid, dwcc.createOutputForCSV(directory, uid)) }.toMap
        } else {
          dataResource2OutputStreams = Map(resourceUid -> dwcc.createOutputForCSV(directory, resourceUid))
        }
        Config.persistenceManager.pageOverSelect("occ", (key, map) => {
          synchronized {
            val dr = map.getOrElse(if (Config.caseSensitiveCassandra) "dataResourceUid" else "dataresourceuid", "")
            val deletedDate = map.getOrElse(if (Config.caseSensitiveCassandra) "deletedDate" else "deleteddate", "")
            if (dr != "" && deletedDate =="") {
              if (dataResource2OutputStreams.get(dr) != None) {
                val dataResourceMap = dataResource2OutputStreams.get(dr)
                if (!dataResourceMap.isEmpty && !dataResourceMap.get.isEmpty) {
                  val (zop, csv) = dataResourceMap.get.get
                  synchronized {
                    val eventDate = {
                      val eventDate = map.getOrElse(if (Config.caseSensitiveCassandra) "eventDate_p" else "eventdate_p" , "")
                      val eventDateEnd = map.getOrElse(if (Config.caseSensitiveCassandra) "eventDateEnd_p" else "eventdateend_p", "")
                      if (eventDateEnd != "" && eventDate != "" && eventDate != eventDateEnd) {
                        eventDate + "/" + eventDateEnd
                      } else {
                        eventDate
                      }
                    }
                    csv.writeNext(Array(
                      cleanValue(map.getOrElse("rowkey", "")),
                      cleanValue(map.getOrElse("occurrenceid", "")),
                      cleanValue(map.getOrElse("basisofrecord_p", "")),
                      cleanValue(map.getOrElse("behavior", "")),
                      cleanValue(map.getOrElse("catalognumber", "")),
                      cleanValue(map.getOrElse("classs_p", "")),
                      cleanValue(map.getOrElse("collectioncode", "")),
                      cleanValue(map.getOrElse("coordinateuncertaintyinmeters_p", "")),
                      cleanValue(map.getOrElse("country_p", "")),
                      cleanValue(map.getOrElse("datageneralizations_p", "")),
                      cleanValue(map.getOrElse("day_p", "")),
                      cleanValue(map.getOrElse("decimallatitude_p", "")),
                      cleanValue(map.getOrElse("decimallongitude_p", "")),
                      cleanValue(map.getOrElse("dynamicproperties", "")),
                      cleanValue(eventDate),
                      cleanValue(map.getOrElse("eventid", "")),
                      cleanValue(map.getOrElse("eventremarks", "")),
                      cleanValue(map.getOrElse("family_p", "")),
                      cleanValue(map.getOrElse("fieldnotes", "")),
                      cleanValue(map.getOrElse("gridreferencewkt_p", "")),
                      cleanValue(map.getOrElse("genus_p", "")),
                      cleanValue(map.getOrElse("geodeticdatum_p", "")),
                      cleanValue(map.getOrElse("georeferenceverificationstatus", "")),
                      cleanValue(map.getOrElse("habitat_p", "")),
                      cleanValue(map.getOrElse("highergeography", "")),
                      cleanValue(map.getOrElse("identificationverificationstatus_p", "")),
                      cleanValue(map.getOrElse("identifiedby", "")),
                      cleanValue(map.getOrElse("individualcount", "")),
                      cleanValue(map.getOrElse("informationwithheld_p", "")),
                      cleanValue(map.getOrElse("institutioncode", "")),
                      cleanValue(map.getOrElse("kingdom_p", "")),
                      cleanValue( licenseToUrlLicense( map.getOrElse("license_p", "") )),
                      cleanValue(map.getOrElse("lifestage", "")),
                      cleanValue(map.getOrElse("locality", "")),
                      cleanValue(map.getOrElse("locationremarks", "")),
                      cleanValue(map.getOrElse("maximumdepthinmeters", "")),
                      cleanValue(map.getOrElse("maximumelevationinmeters", "")),
                      cleanValue(map.getOrElse("measurementmethod", "")),
                      cleanValue(map.getOrElse("measurementtype", "")),
                      cleanValue(map.getOrElse("measurementunit", "")),
                      cleanValue(map.getOrElse("measurementvalue", "")),
                      cleanValue(map.getOrElse("minimumdepthinmeters", "")),
                      cleanValue(map.getOrElse("minimumelevationinmeters", "")),
                      cleanValue(map.getOrElse("month_p", "")),
                      cleanValue(map.getOrElse("nomenclaturalstatus_p", "")),
                      cleanValue(map.getOrElse("occurrenceremarks", "")),
                      cleanValue(map.getOrElse("occurrencestatus_p", "")),
                      cleanValue(map.getOrElse("order_p", "")),
                      cleanValue(map.getOrElse("organismquantity", "")),
                      cleanValue(map.getOrElse("organismquantitytype", "")),
                      cleanValue(map.getOrElse("organismremarks", "")),
                      cleanValue(map.getOrElse("organismscope", "")),
                      cleanValue(map.getOrElse("phylum_p", "")),
                      cleanValue(map.getOrElse("recordedby_p", "")),
                      cleanValue(map.getOrElse("recordnumber", "")),
                      cleanValue(map.getOrElse("rightsholder", "")),
                      cleanValue(map.getOrElse("samplesizeunit", "")),
                      cleanValue(map.getOrElse("samplesizevalue", "")),
                      cleanValue(map.getOrElse("samplingeffort", "")),
                      cleanValue(map.getOrElse("samplingprotocol", "")),
                      cleanValue(map.getOrElse("scientificname_p", "")),
                      cleanValue(map.getOrElse("scientificnameauthorship_p", "")),
                      cleanValue(map.getOrElse("sex", "")),
                      cleanValue(map.getOrElse("stateprovince_p", "")),
                      cleanValue(map.getOrElse("taxonconceptid_p", "")),
                      cleanValue(map.getOrElse("taxonid", "")),
                      cleanValue(map.getOrElse("taxonrank_p", "")),
                      cleanValue(map.getOrElse("verbatimdepth", "")),
                      cleanValue(map.getOrElse("vernacularname_p", "")),
                      cleanValue(map.getOrElse("year_p", ""))

                    /* original exported values pre 2/4/20

                      cleanValue(map.getOrElse("rowkey", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "catalogNumber" else "catalognumber", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "collectionCode" else "collectioncode", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "institutionCode" else "institutioncode", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "recordNumber" else "recordnumber", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "basisOfRecord_p" else "basisofrecord_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "recordedBy" else "recordedby", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "occurrenceStatus_p" else "occurrencestatus_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "individualCount" else "individualcount", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "scientificName_p" else "scientificname_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "taxonConceptID_p" else "taxonconceptid_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "taxonRank_p" else "taxonrank_p", "")),
                      cleanValue(map.getOrElse("kingdom_p", "")),
                      cleanValue(map.getOrElse("phylum_p", "")),
                      cleanValue(map.getOrElse("classs_p", "")),
                      cleanValue(map.getOrElse("order_p", "")),
                      cleanValue(map.getOrElse("family_p", "")),
                      cleanValue(map.getOrElse("genus_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "vernacularName_p" else "vernacularname_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "decimalLatitude_p" else "decimallatitude_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "decimalLongitude_p" else "decimallongitude_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "geodeticDatum_p" else "geodeticdatum_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "coordinateUncertaintyInMeters_p" else "coordinateuncertaintyinmeters_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "maximumElevationInMeters" else "maximumelevationinmeters", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "minimumElevationInMeters" else "minimumelevationinmeters", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "minimumDepthInMeters" else "minimumdepthinmeters", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "maximumDepthInMeters" else "maximumdepthinmeters", "")),
                      cleanValue(map.getOrElse("country_p", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "stateProvince_p" else "stateprovince_p", "")),
                      cleanValue(map.getOrElse("locality", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "locationRemarks" else "locationremarks", "")),
                      cleanValue(map.getOrElse("year_p", "")),
                      cleanValue(map.getOrElse("month_p", "")),
                      cleanValue(map.getOrElse("day_p", "")),
                      cleanValue(eventDate),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "eventID" else "eventid", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "identifiedBy" else "identifiedby", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "occurrenceRemarks" else "occurrenceremarks", "")),
                      cleanValue(map.getOrElse(if (Config.caseSensitiveCassandra) "dataGeneralizations_p" else "datageneralizations_p", ""))

                     */
                    ))
                    csv.flush()
                  }
                }
              }
            }
          }
          true
        }, threads, 1000, defaultFields:_*)

        dataResource2OutputStreams.values.foreach { zopAndCsv =>
          if (zopAndCsv != None) {
            zopAndCsv.get._1.flush()
            zopAndCsv.get._1.closeEntry()
            zopAndCsv.get._1.close()
          }
        }
      } catch {
        case e:Exception => {
          logger.error(e.getMessage(), e)
          throw new RuntimeException(e)
        }
      }
    }
  }

  def cleanValue(input:String) = if(input == null) "" else input.replaceAll("[\\t\\n\\r]", " ").trim

  def licenseToUrlLicense( license : String ) : String = {
    if (license == "CC0")
      "https://creativecommons.org/publicdomain/zero/1.0/legalcode"
    else if (license == "CC-BY")
      "https://creativecommons.org/licenses/by/4.0/legalcode"
    else if (license == "CC-BY-NC")
      "https://creativecommons.org/licenses/by-nc/4.0/legalcode"
    else
      ""
  }

  // pattern to extract a data resource uid from a filter query , because the label show i18n value
  val dataResourcePattern = "(?:[\"]*)?(?:[a-z_]*_uid:\")([a-z0-9]*)(?:[\"]*)?".r

  def getDataResourceUids : Seq[String] = {
    val url = Config.biocacheServiceUrl + "/occurrences/search?q=*:*&facets=data_resource_uid&pageSize=0&flimit=-1"
    val jsonString = Source.fromURL(url).getLines.mkString
    val json = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
    val results = json.get("facetResults").get.asInstanceOf[List[Map[String, String]]].head.get("fieldResult").get.asInstanceOf[List[Map[String, String]]]
    results.map(facet => {
      val fq = facet.get("fq").get
      parseFq(fq)
    }).filterNot(_.equals("Unknown"))
  }

  def parseFq(fq: String): String = fq match {
    case dataResourcePattern(dr) => dr
    case _ => "Unknown"
  }
}

/**
 * Class for creating a Darwin Core Archive from data in the biocache.
 *
 * TODO support for dwc fields in registry metadata. When not available use the default fields.
 */
class DwCACreator {

  val logger = LoggerFactory.getLogger("DwCACreator")


  def createOutputForCSV(directory:String, dataResource:String) : Option[(ZipOutputStream, CSVWriter)] = {

    logger.info("Creating archive for " + dataResource)
    val zipFile = new java.io.File (
      directory +
      System.getProperty("file.separator") +
      dataResource +
      System.getProperty("file.separator") +
      dataResource +
      ".zip"
    )

    FileUtils.forceMkdir(zipFile.getParentFile)
    val zop = new ZipOutputStream(new FileOutputStream(zipFile))
    if(addEML(zop, dataResource)){
      addMeta(zop)
      zop.putNextEntry(new ZipEntry("occurrence.csv"))
      val writer = new CSVWriter(new OutputStreamWriter(zop))
      Some((zop, writer))
    } else {
      //no EML implies that a DWCA should not be generated.
      zop.close()
      FileUtils.deleteQuietly(zipFile)
      None
    }
  }

  def addEML(zop:ZipOutputStream, dr:String):Boolean ={
    try {
      zop.putNextEntry(new ZipEntry("eml.xml"))
      val content = Source.fromURL(Config.registryUrl + "/eml/" + dr).mkString
      zop.write(content.getBytes)
      zop.flush
      zop.closeEntry
      true
    } catch {
      case e:Exception => e.printStackTrace();false
    }
  }

  def addMeta(zop:ZipOutputStream) ={
    zop.putNextEntry(new ZipEntry("meta.xml"))
    val metaXml = <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
      <core encoding="UTF-8" linesTerminatedBy="\r\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="0" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
      <files>
            <location>occurrence.csv</location>
      </files>

        <id index="0"/>
        <field index="1"  term="http://rs.tdwg.org/dwc/terms/occurrenceID" />
        <field index="2"  term="http://rs.tdwg.org/dwc/terms/basisOfRecord" default="HumanObservation"/>
        <field index="3"  term="http://rs.tdwg.org/dwc/terms/behavior" />
        <field index="4"  term="http://rs.tdwg.org/dwc/terms/catalogNumber"/>
        <field index="5"  term="http://rs.tdwg.org/dwc/terms/class"/>
        <field index="6"  term="http://rs.tdwg.org/dwc/terms/collectionCode"/>
        <field index="7"  term="http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters"/>
        <field index="8"  term="http://rs.tdwg.org/dwc/terms/country"/>
        <field index="9"  term="http://rs.tdwg.org/dwc/terms/dataGeneralizations"/>
        <field index="10"  term="http://rs.tdwg.org/dwc/terms/day"/>
        <field index="11"  term="http://rs.tdwg.org/dwc/terms/decimalLatitude"/>
        <field index="12"  term="http://rs.tdwg.org/dwc/terms/decimalLongitude"/>
        <field index="13"  term="http://rs.tdwg.org/dwc/terms/dynamicProperties"/>
        <field index="14"  term="http://rs.tdwg.org/dwc/terms/eventDate"/>
        <field index="15"  term="http://rs.tdwg.org/dwc/terms/eventID"/>
        <field index="16"  term="http://rs.tdwg.org/dwc/terms/eventRemarks"/>
        <field index="17"  term="http://rs.tdwg.org/dwc/terms/family"/>
        <field index="18"  term="http://rs.tdwg.org/dwc/terms/fieldNotes"/>
        <field index="19"  term="http://rs.tdwg.org/dwc/terms/footprintWKT"/>
        <field index="20"  term="http://rs.tdwg.org/dwc/terms/genus"/>
        <field index="21"  term="http://rs.tdwg.org/dwc/terms/geodeticDatum"/>
        <field index="22"  term="http://rs.tdwg.org/dwc/terms/georeferenceVerificationStatus"/>
        <field index="23"  term="http://rs.tdwg.org/dwc/terms/habitat"/>
        <field index="24"  term="http://rs.tdwg.org/dwc/terms/higherGeography"/>
        <field index="25"  term="http://rs.tdwg.org/dwc/terms/identificationRemarks"/>
        <field index="26"  term="http://rs.tdwg.org/dwc/terms/identificationVerificationStatus"/>
        <field index="27"  term="http://rs.tdwg.org/dwc/terms/identifiedBy"/>
        <field index="28"  term="http://rs.tdwg.org/dwc/terms/individualCount"/>
        <field index="29"  term="http://rs.tdwg.org/dwc/terms/informationWithheld"/>
        <field index="30"  term="http://rs.tdwg.org/dwc/terms/institutionCode"/>
        <field index="31"  term="http://rs.tdwg.org/dwc/terms/kingdom"/>
        <field index="32"  term="http://rs.tdwg.org/dwc/terms/license"/>
        <field index="33"  term="http://rs.tdwg.org/dwc/terms/lifeStage"/>
        <field index="34"  term="http://rs.tdwg.org/dwc/terms/locality"/>
        <field index="35"  term="http://rs.tdwg.org/dwc/terms/locationRemarks"/>
        <field index="36"  term="http://rs.tdwg.org/dwc/terms/maximumDepthInMeters"/>
        <field index="37"  term="http://rs.tdwg.org/dwc/terms/maximumElevationInMeters"/>
        <field index="38"  term="http://rs.tdwg.org/dwc/terms/measurementMethod"/>
        <field index="39"  term="http://rs.tdwg.org/dwc/terms/measurementType"/>
        <field index="40"  term="http://rs.tdwg.org/dwc/terms/measurementUnit"/>
        <field index="41"  term="http://rs.tdwg.org/dwc/terms/measurementValue"/>
        <field index="42"  term="http://rs.tdwg.org/dwc/terms/minimumDepthInMeters"/>
        <field index="43"  term="http://rs.tdwg.org/dwc/terms/minimumElevationInMeters"/>
        <field index="44"  term="http://rs.tdwg.org/dwc/terms/month"/>
        <field index="45"  term="http://rs.tdwg.org/dwc/terms/nomenclaturalStatus"/>
        <field index="46"  term="http://rs.tdwg.org/dwc/terms/occurrenceRemarks"/>
        <field index="47"  term="http://rs.tdwg.org/dwc/terms/occurrenceStatus"/>
        <field index="48"  term="http://rs.tdwg.org/dwc/terms/order"/>
        <field index="49"  term="http://rs.tdwg.org/dwc/terms/organismQuantity"/>
        <field index="50"  term="http://rs.tdwg.org/dwc/terms/organismQuantityType"/>
        <field index="51"  term="http://rs.tdwg.org/dwc/terms/organismQuantityRemarks"/>
        <field index="52"  term="http://rs.tdwg.org/dwc/terms/organismScope"/>
        <field index="53"  term="http://rs.tdwg.org/dwc/terms/phylum"/>
        <field index="54"  term="http://rs.tdwg.org/dwc/terms/recordedBy"/>
        <field index="55"  term="http://rs.tdwg.org/dwc/terms/recordNumber"/>
        <field index="56"  term="http://rs.tdwg.org/dwc/terms/rightsHolder"/>
        <field index="57"  term="http://rs.tdwg.org/dwc/terms/sampleSizeUnit"/>
        <field index="58"  term="http://rs.tdwg.org/dwc/terms/sampleSizeValue"/>
        <field index="59"  term="http://rs.tdwg.org/dwc/terms/samplingEffort"/>
        <field index="60"  term="http://rs.tdwg.org/dwc/terms/samplingProtocol"/>
        <field index="61"  term="http://rs.tdwg.org/dwc/terms/scientificName"/>
        <field index="62"  term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
        <field index="63"  term="http://rs.tdwg.org/dwc/terms/sex"/>
        <field index="64"  term="http://rs.tdwg.org/dwc/terms/stateProvince"/>
        <field index="65"  term="http://rs.tdwg.org/dwc/terms/taxonConceptID"/>
        <field index="66"  term="http://rs.tdwg.org/dwc/terms/taxonID"/>
        <field index="67"  term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
        <field index="68"  term="http://rs.tdwg.org/dwc/terms/verbatimDepth"/>
        <field index="69"  term="http://rs.tdwg.org/dwc/terms/vernacularName"/>
        <field index="70"  term="http://rs.tdwg.org/dwc/terms/year"/>

      </core>
    </archive>
    //add the XML
    zop.write("""<?xml version="1.0"?>""".getBytes)
    zop.write("\n".getBytes)
    zop.write(metaXml.mkString("\n").getBytes)
    zop.flush
    zop.closeEntry
  }
}


package au.org.ala.biocache

import java.io.File
import java.net.URI
import java.nio.file.Paths

import au.org.ala.biocache.index.IndexLocalNode
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.load.FullRecordMapper.fullRecord2Map
import au.org.ala.biocache.model.{FullRecord, Versions}
import au.org.ala.biocache.processor.{ClassificationProcessor, DefaultValuesProcessor, LocationProcessor, LocationTwoTierPreProcessor, RecordProcessor, SensitivityProcessor}
import org.apache.commons.io.FileUtils
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.FSDirectory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TwoTierIndexingTest  extends ConfigFunSuite {

  test("Two tier indexing"){

    val workingDir = System.getProperty("user.dir")
    val merged = new File(workingDir + "/src/test/resources/solr/biocache/merged_0")
    val confTmp = new File(workingDir + "/src/test/resources/solr/biocache/solr-create")

    val raw = new FullRecord
    val processed = new FullRecord

    //needed for sensitivity processor
    raw.location.country = "United Kingdom"
    raw.location.stateProvince = "England"

    raw.occurrence.highResolution = "1"
    raw.occurrence.highResolutionNBNtoBlur = "t"
    raw.classification.taxonID = "NHMSYS0021276106" //"NBNSYS0000000136"
    raw.classification.scientificName = "Spatula querquedula" //Anas querquedula
    raw.location.decimalLatitude = "52.712"
    raw.location.decimalLongitude = "-2.756"
    raw.location.coordinateUncertaintyInMeters = "500"
    raw.location.gridReference = "SJ4913"
    raw.location.gridSizeInMeters = "1000"

    //for indexing
    raw.rowKey = "1"
    raw.attribution.dataResourceUid = "dr9999"

    //val rp = new RecordProcessor
    //rp.processRecord(raw,processed)

    (new LocationTwoTierPreProcessor).process("test", raw, processed)
    (new LocationProcessor).process("test", raw, processed)
    (new SensitivityProcessor).process("test", raw, processed)

    if (merged.exists()) FileUtils.deleteDirectory(merged)
    if (confTmp.exists()) FileUtils.deleteDirectory(confTmp)

    val pm = Config.persistenceManager
    // load some test data
    var rawAsMap = (FullRecordMapper.fullRecord2Map(raw, Versions.RAW) += ("rowkey" -> "1")).toMap
    pm.put(raw.rowKey, "occ", rawAsMap, false, false)
    var processedAsMap = (FullRecordMapper.fullRecord2Map(processed, Versions.PROCESSED) += ("rowkey" -> "1")).toMap
    pm.put(raw.rowKey, "occ", processedAsMap, false, false)


    val i = new IndexLocalNode
    i.indexRecords(1,
      workingDir + "/src/test/resources/solr/biocache/", //solrHome: String,
      workingDir + "/src/test/resources/solr/biocache/conf/solrconfig.xml",
      true, //optimise: Boolean,
      false, //optimiseOnly: Boolean,
      "", //checkpointFile: String,
      1, //threadsPerWriter: Int,
      1, //threadsPerProcess: Int,
      500, //ramPerWriter: Int,
      100, //writerSegmentSize: Int,
      1000, //processorBufferSize: Int,
      1000, //writerBufferSize: Int,
      100, //pageSize: Int,
      1, //mergeSegments: Int,
      false, //test: Boolean,
      1, //writerCount: Int,
      false, //testMap: Boolean,
      1 //maxRecordsToIndex:Int = -1
    )

    //verify the index
    val indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(new URI("file://" + workingDir + "/src/test/resources/solr/biocache/merged_0"))))

    val maxDocId = indexReader.maxDoc()
    val doc = indexReader.document(0)

    indexReader.close

    expectResult("true"){ doc.get("highresolution") }
    expectResult("52.7"){ doc.get("latitude") }
    expectResult("52.712"){ doc.get("sensitive_latitude") }
    expectResult("SJ4913"){ doc.get("sensitive_grid_reference") }
    expectResult("1000"){ doc.get("sensitive_grid_size") }
    expectResult("generalised"){ doc.get("sensitive") }


    if(merged.exists()) FileUtils.deleteDirectory(merged)
    if(confTmp.exists()) FileUtils.deleteDirectory(confTmp)
  }
}

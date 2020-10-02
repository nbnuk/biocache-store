package au.org.ala.biocache.load

import java.io.File
import au.org.ala.biocache.{Config, ConfigFunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TwoTierLoaderTest extends ConfigFunSuite {
    val WORK_FILE = new File("src/test/resources/au/org/ala/load/test-twotier/occurrences.csv")
    val DATA_RESOURCE_UID = "dr9999"
    val UNIQUE_FIELDS = Array("occurrenceID")
    val LOG_ROW_KEYS = true
    val TEST_FILE = false
    val TOT_RECS = 24

    test("load two-tier CSV dataset") {
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

        //now check if records have been added
        val rowkeyMap = Config.persistenceManager.get("dr9999|1", "occ_uuid")
        val rowkey = rowkeyMap.get("value")
        val occID = Config.persistenceManager.get(rowkey,"occ","occurrenceID")
        expectResult(Some("1")) { occID }

        val rowkeyMap2 = Config.persistenceManager.get("dr9999|24", "occ_uuid")
        val rowkey2 = rowkeyMap2.get("value")
        val occID2 = Config.persistenceManager.get(rowkey2,"occ","occurrenceID")
        expectResult(Some("24")) { occID2 }

        val rowkeyMap3 = Config.persistenceManager.get("dr9999|18", "occ_uuid")
        val rowkey3 = rowkeyMap3.get("value")
        val occ = Config.persistenceManager.get(rowkey3,"occ")
        expectResult("1") { occ.get("highResolution") }
        expectResult("5") { occ.get("highResolutionCoordinateUncertaintyInMeters") }
        expectResult("SJ49371372") { occ.get("highResolutionGridReference") }
        expectResult("52.71875") { occ.get("highResolutionDecimalLatitude") }
        expectResult("52.71875") { occ.get("highResolutionDecimalLatitude") }
        expectResult("High resolution locality") { occ.get("highResolutionLocality") }
        expectResult("HighRes Event ID 18") { occ.get("highResolutionEventID") }

    }


}

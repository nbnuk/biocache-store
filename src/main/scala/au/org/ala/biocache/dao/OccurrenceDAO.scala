package au.org.ala.biocache.dao

import au.org.ala.biocache._
import java.io.OutputStream
import au.org.ala.biocache.model.{QualityAssertion, Version, FullRecord}
import au.org.ala.biocache.vocab.ErrorCode

/**
 * DAO for occurrences. This DAO handles the full and partial persistence and retrieval of occurrence records.
 */
trait OccurrenceDAO extends DAO {

  val entityName = "occ"

  val qaEntityName = "qa"

  def createUniqueID(dataResourceUid:String, identifyingTerms:Seq[String], stripSpaces:Boolean=false) : String

  def setDeleted(rowKey: String, del: Boolean, dateTime:Option[String]=None) : Unit

  def rowKeyExists(uuid:String) : Boolean

  def getRowKeyFromUuid(uuid:String) : Option[String]

  def getByRowKey(rowKey: String) : Option[FullRecord] = getByRowKey(rowKey, false)

  def getByRowKey(rowKey: String, includeSensitive:Boolean) : Option[FullRecord]

  def getAllVersionsByRowKey(rowKey:String, includeSensitive:Boolean=false) : Option[Array[FullRecord]]

  def getRawProcessedByRowKey(rowKey:String) : Option[Array[FullRecord]]

  def getByUuid(uuid: String, version: Version, includeSensitive:Boolean=false): Option[FullRecord]

  def getByRowKey(rowKey: String, version:Version, includeSensitive:Boolean=false): Option[FullRecord]

  def getUUIDForUniqueID(uniqueID: String) : Option[String]

  def createOrRetrieveUuid(uniqueID: String): (String, Boolean)

  def writeToStream(outputStream: OutputStream, fieldDelimiter: String, recordDelimiter: String, rowKeys: Array[String], fields: Array[String], qaFields:Array[String], includeSensitive:Boolean=false): Unit

  def writeToRecordWriter(writer:RecordWriter, rowKeys: Array[String], fields: Array[String], qaFields:Array[String], includeSensitive:Boolean=false, includeMisc:Boolean=false, miscFields:Array[String]=null, dataToInsert:java.util.Map[String, Array[String]] = null, explainLicense: String = null): Array[String]

  def pageOverAllVersions(proc: ((Option[Array[FullRecord]]) => Boolean), dataResourceUid: String, pageSize: Int = 1000): Unit

  def pageOverAll(version: Version, proc: ((Option[FullRecord]) => Boolean), dataResourceUid: String, pageSize: Int = 1000): Unit

  def pageOverRawProcessed(proc: (Option[(FullRecord, FullRecord)] => Boolean), dataResourceUid: String, pageSize: Int = 1000, threads: Int = 4): Unit

  def pageOverRawProcessedLocal(proc: (Option[(FullRecord, FullRecord)] => Boolean), dataResourceUid: String, threads: Int = 4): Int

  def addRawOccurrence(fullRecord: FullRecord,removeNullFields:Boolean): Unit

  def addRawOccurrenceBatch(fullRecords: Array[FullRecord], removeNullFields:Boolean=false): Unit

  def updateOccurrence(rowKey: String, fullRecord: FullRecord, assertions: Option[Map[String,Array[QualityAssertion]]], version: Version): Unit

  def updateOccurrence(rowKey: String, oldRecord: FullRecord, updatedRecord: FullRecord, assertions: Option[Map[String,Array[QualityAssertion]]], version: Version)

  def updateOccurrenceBatch(batches: List[Map[String, Object]])

  def addSystemAssertion(rowKey: String, qualityAssertion: QualityAssertion, replaceExistCode:Boolean=false, checkExisting:Boolean=true): Unit

  def removeSystemAssertion(rowKey: String, errorCode:ErrorCode) : Unit

  def updateSystemAssertions(rowKey: String, qualityAssertions: Map[String,Array[QualityAssertion]]): Unit

  def getSystemAssertions(rowKey: String): List[QualityAssertion]

  def addUserAssertion(rowKey: String, qualityAssertion: QualityAssertion): Unit

  def getUserAssertions(rowKey: String): List[QualityAssertion]

  def getUserIdsForAssertions(rowKey: String) : Set[String]

  def deleteUserAssertion(rowKey: String, assertionUuid: String): Boolean

  def updateAssertionStatus(rowKey: String, assertion: QualityAssertion, systemAssertions: List[QualityAssertion], userAssertions: List[QualityAssertion])

  def reIndex(rowKey: String)

  def delete(rowKey: String, removeFromIndex:Boolean = true, logDeleted:Boolean = false) : Boolean

  def downloadMedia(fr:FullRecord) : Boolean

  def isSensitive(fr:FullRecord) : Boolean
}

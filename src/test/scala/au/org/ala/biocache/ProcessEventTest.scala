package au.org.ala.biocache
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.commons.lang.time.DateUtils
import java.util.Date
import java.text.SimpleDateFormat
import au.org.ala.biocache.processor.EventProcessor
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.util.DateUtil

/**
 * Tests for event date parsing. To run these tests create a new scala application
 * run configuration in your IDE.
 *
 * See http://www.scalatest.org/getting_started_with_fun_suite
 *
 * scala -cp scalatest-1.0.jar org.scalatest.tools.Runner -p . -o -s ay.au.biocache.ProcessEventTests
 *
 * @author Dave Martin (David.Martin@csiro.au)
 */
@RunWith(classOf[JUnitRunner])
class ProcessEventTest extends ConfigFunSuite {

  test("00 month test"){
    val raw = new FullRecord("1234")
    raw.event.day ="0"
    raw.event.month = "0"
    raw.event.year = "0"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)
  }

  test("yyyy-dd-mm correctly sets year, month, day values in process object") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1978-12-31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("yyyy-dd-mm verbatim date correctly sets year, month, day values in process object") {

    val raw = new FullRecord("1234")
    raw.event.verbatimEventDate = "1978-12-31/1978-12-31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }

    //identical start and end dates cause eventDateEnd not set
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("if year, day, month supplied, eventDate is correctly set") {

    val raw = new FullRecord("1234")
    raw.event.year = "1978"
    raw.event.month = "12"
    raw.event.day = "31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
  }

  test("if year supplied in 'yy' format, eventDate is correctly set") {

    val raw = new FullRecord("1234")
    raw.event.year = "78"
    raw.event.month = "12"
    raw.event.day = "31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
  }

  test("day month transposed") {

    val raw = new FullRecord("1234")
    raw.event.year = "78"
    raw.event.month = "16"
    raw.event.day = "6"
    val processed = raw.clone
    val assertions = (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-06-16"){ processed.event.eventDate }
    expectResult("16"){ processed.event.day }
    expectResult("06"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
    //expectResult(1){ assertions.size }
    expectResult(0){ assertions.find(_.code == 30009).get.qaStatus }
  }

  test("invalid month test") {

    val raw = new FullRecord( "1234")
    val processed = new FullRecord("1234")
    raw.event.year = "78"
    raw.event.month = "16"
    raw.event.day = "16"

    val assertions = (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult("16"){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult("1978"){ processed.event.year }

    //expectResult(1){ assertions.size }
    expectResult(0){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test("invalid month test > 12") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.year = "1978"
    raw.event.month = "40"
    raw.event.day = "16"

    val assertions = (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult("16"){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult("1978"){ processed.event.year }

    //expectResult(1){ assertions.size }
    expectResult(0){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test("year = 11, month = 02, day = 01") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.year = "11"
    raw.event.month = "02"
    raw.event.day = "01"

    val assertions = (new EventProcessor).process("1234", raw, processed)

    expectResult("2011-02-01"){ processed.event.eventDate }
    expectResult("1"){ processed.event.day }
    expectResult("02"){ processed.event.month }
    expectResult("2011"){ processed.event.year }

    //expectResult(0){ assertions.size }
    expectResult(1){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test("1973-10-14") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "1973-10-14"

    val assertions = (new EventProcessor).process("1234", raw, processed)

    expectResult("1973-10-14"){ processed.event.eventDate }
    expectResult("14"){ processed.event.day }
    expectResult("10"){ processed.event.month }
    expectResult("1973"){ processed.event.year }

    //expectResult(0){ assertions.size }
    expectResult(1){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test("today"){
    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    val sf = new SimpleDateFormat("yyyy-MM-dd")
    raw.event.eventDate = sf.format(new Date())
    val assertions = (new EventProcessor).process("1234", raw, processed)
    expectResult(DateUtil.getCurrentYear.toString){ processed.event.year }
    //expectResult(0){ assertions.size }
    expectResult(1){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test("tomorrow"){
    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    val sf = new SimpleDateFormat("yyyy-MM-dd")
    raw.event.eventDate = sf.format(DateUtils.addDays(new Date(),1))
    val assertions = (new EventProcessor).process("1234", raw, processed)
    expectResult(DateUtil.getCurrentYear.toString){ processed.event.year }
    expectResult(true){ assertions.size > 0 }
    expectResult(0){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test("a digit year which gives a future date") {
    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    val futureDate = DateUtils.addDays(new Date(),2)

    val twoDigitYear =(new SimpleDateFormat("yy")).format(futureDate)

    raw.event.year = (new SimpleDateFormat("yy")).format(futureDate)
    raw.event.month = (new SimpleDateFormat("MM")).format(futureDate)
    raw.event.day = (new SimpleDateFormat("dd")).format(futureDate)

    val assertions = (new EventProcessor).process("1234", raw, processed)

    expectResult("19"+twoDigitYear){ processed.event.year }

    //expectResult(0){ assertions.size }
    expectResult(1){ assertions.find(_.code == 30007).get.qaStatus }
  }

  test ("Identification predates the occurrence") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.identification.dateIdentified = "2012-01-01"
    raw.event.eventDate = " 2013-01-01"

    var qas = (new EventProcessor).process("test", raw, processed)
    expectResult(0) {
      //the identification happened before the collection !!
      qas.find {_.getName == "idPreOccurrence"}.get.qaStatus
    }

    raw.identification.dateIdentified = "2013-01-01"
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult(1) {
      //the identification happened at the same time of the collection
      qas.find {_.getName == "idPreOccurrence"}.get.qaStatus
    }
  }

  test ("Georeferencing postdates the occurrence") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.georeferencedDate = "2013-04-01"
    raw.event.eventDate = " 2013-01-01"

    var qas = (new EventProcessor).process("test", raw, processed)
    expectResult(0) {
      //the georeferencing happened after the collection !!
      qas.find {_.getName == "georefPostDate"}.get.qaStatus
    }

    raw.location.georeferencedDate = "2013-01-01"
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult(1) {
      //the georeferecing happened at the same time as the collection
      qas.find {_.getName == "georefPostDate"}.get.qaStatus
    }
  }

  test("First of dates") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.event.day ="1"
    raw.event.month="1"
    raw.event.year="2000"

    var qas = (new EventProcessor).process("test", raw, processed)
    expectResult(0) {
      //date is first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(0) {
      //date is also the first of the year
      qas.find {_.getName == "firstOfYear"}.get.qaStatus
    }
    expectResult(0) {
      //date is also the first of the century
      qas.find {_.getName == "firstOfCentury"}.get.qaStatus
    }

    raw.event.year="2001"
    processed = new FullRecord
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult(0) {
      //date is first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(0) {
      //date is also the first of the year
      qas.find {_.getName == "firstOfYear"}.get.qaStatus
    }
    expectResult(1) {
      //date is NOT the first of the century
      qas.find {_.getName == "firstOfCentury"}.get.qaStatus
    }

    raw.event.month="2"
    processed = new FullRecord
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult(0) {
      //date is first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(1) {
      //date is NOT the first of the year
      qas.find {_.getName == "firstOfYear"}.get.qaStatus
    }
    expectResult(None) {
      //date is NOT the first of the century  - not tested since the month is not January
      qas.find {_.getName == "firstOfCentury"}
    }

    raw.event.day = "2"
    processed = new FullRecord
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult(1) {
      //date is NOT first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(None) {
      //date is NOT the first of the year - gtested since the day is not 1
      qas.find {_.getName == "firstOfYear"}
    }
    expectResult(None) {
      //date is NOT the first of the century - not tested since the month is not January
      qas.find {_.getName == "firstOfCentury"}
    }
  }

  test("Year only - results in incomplete date error but NOT invalid date"){
    val raw = new FullRecord
    val processed = new FullRecord

    raw.event.eventDate="1978"

    val qas = (new EventProcessor).process("test",raw,processed)
    //AssertionCodes.INVALID_COLLECTION_DATE
    println(qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code))
    println(qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code))
  }

  test("Incomplete Date Tests"){

    //valid but incomlete event year
    var raw = new FullRecord
    val processed = new FullRecord
    raw.event.year="2014"
    var qas = (new EventProcessor).process("test",raw,processed)
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

    //valid and complete day, month and year
    raw.event.month="01"
    raw.event.day="11"
    qas = (new EventProcessor).process("test",raw,processed)
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

    //valid but incomplete eventDate
    raw = new FullRecord
    raw.event.eventDate="2014-02"
    qas = (new EventProcessor).process("test",raw,processed)
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

    //invalid date
    raw.event.eventDate="2012-22"
    qas = (new EventProcessor).process("test",raw,processed)
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

    //valid and complete event date
    raw.event.eventDate="2014-02-15"
    qas = (new EventProcessor).process("test",raw,processed)
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

    //valid and incomplete verbatim event date
    raw = new FullRecord
    raw.event.verbatimEventDate="2014-02"
    qas = (new EventProcessor).process("test",raw,processed)
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

    //valid and complete verbatim event date
    raw.event.verbatimEventDate="2014-02-15"
    qas = (new EventProcessor).process("test",raw,processed)
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}

  }

  test("First Fleet Tests"){
    var raw = new FullRecord
    val processed = new FullRecord
    raw.event.year="1788"
    raw.event.month="01"
    raw.event.day="26"
    var qas = (new EventProcessor).process("test", raw, processed)
    expectResult("First Fleet arrival implies a null date"){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getComment}

    raw = new FullRecord
    raw.event.eventDate = "1788-01-26"
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult("First Fleet arrival implies a null date"){qas.find(_.code == au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getComment}
  }

  test("00 month and day"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2014-00-00"
    //should be an invalid date
  }

  test("if year, day, month, eventDate supplied, eventDate is used for eventDateEnd") {

    val raw = new FullRecord( "1234")
    raw.event.eventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult(null){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult(null){ processed.event.year }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("if year, day, month, verbatimEventDate supplied, verbatimEventDate is used for eventDateEnd") {

    val raw = new FullRecord("1234")
    raw.event.year = "1978"
    raw.event.month = "12"
    raw.event.day = "31"
    raw.event.verbatimEventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult(null){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult(null){ processed.event.year }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("if eventDate supplied, eventDate is used for eventDateEnd") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("if full date range check end is not before start") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1979-01-02/1978-12-31"
    val processed = raw.clone
    var qas = (new EventProcessor).process("1234", raw, processed)

    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("if month date range check end is not before start") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1979-01/1978-12"
    val processed = raw.clone
    var qas = (new EventProcessor).process("1234", raw, processed)

    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("if year date range check end is not before start") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1979/1978"
    val processed = raw.clone
    var qas = (new EventProcessor).process("1234", raw, processed)

    expectResult(1){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(0){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("if verbatimEventDate supplied, verbatimEventDate is used for eventDateEnd") {

    val raw = new FullRecord("1234")
    raw.event.verbatimEventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("separate start end end dates") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "31/12/1978"
    raw.event.eventDateEnd = "02/01/1979"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("separate start end dates - month precision") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "01/12/1978"
    raw.event.eventDateEnd = "31/12/1978"
    raw.event.datePrecision = "M"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12"){ processed.event.eventDate }
    expectResult("1978-12"){ processed.event.eventDateEnd }
    expectResult("Month"){ processed.event.datePrecision }
  }

  test("separate start end dates - day precision") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "01/12/1978"
    raw.event.eventDateEnd = "01/12/1978"
    raw.event.datePrecision = "D"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-01"){ processed.event.eventDate }
    expectResult("1978-12-01"){ processed.event.eventDateEnd }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("01"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
  }

  test("separate start end dates - day precision 2") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "04/08/2009"
    raw.event.eventDateEnd = "04/08/2009"
    raw.event.datePrecision = "Day"

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2009-08-04"){ processed.event.eventDate }
    expectResult("2009-08-04"){ processed.event.eventDateEnd }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("04"){ processed.event.day }
    expectResult("08"){ processed.event.month }
    expectResult("2009"){ processed.event.year }
  }

  test("separate start end dates - day precision 2 - year range") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "01/01/2005"
    raw.event.eventDateEnd = "31/12/2009"
    raw.event.datePrecision = "YY"

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2005"){ processed.event.eventDate }
    expectResult("2009"){ processed.event.eventDateEnd }
    expectResult("Year Range"){ processed.event.datePrecision }
    expectResult(null){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult(null){ processed.event.year }
  }

    test("invalid date" ) {

      val raw = new FullRecord("1234")
      val processed = new FullRecord("1234")
      raw.event.eventDate = "26-6-5"
      raw.event.eventDateEnd = null
      raw.event.datePrecision = null

      (new EventProcessor).process("1234", raw, processed)

      expectResult("2005-06-26"){ processed.event.eventDate }
      expectResult(null){ processed.event.eventDateEnd }
      expectResult("Day"){ processed.event.datePrecision }
      expectResult("26"){ processed.event.day }
      expectResult("06"){ processed.event.month }
      expectResult("2005"){ processed.event.year }
    }

  test("ambiguous date 26-6-5" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "26-6-5"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2005-06-26"){ processed.event.eventDate }
    expectResult(null){ processed.event.eventDateEnd }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("26"){ processed.event.day }
    expectResult("06"){ processed.event.month }
    expectResult("2005"){ processed.event.year }
  }

  test("ambiguous date 24-6-2" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "24-6-2"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2002-06-24"){ processed.event.eventDate }
    expectResult(null){ processed.event.eventDateEnd }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("24"){ processed.event.day }
    expectResult("06"){ processed.event.month }
    expectResult("2002"){ processed.event.year }
  }

  test("24-5-26 unparseable" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "24-5-26"
    raw.event.verbatimEventDate = "24-5-26"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult("24"){ processed.event.day }
    expectResult("05"){ processed.event.month }
    expectResult(null){ processed.event.year }
  }

  test("2002-02-02 eventDate, 02/02/2 verbatim" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = null
    raw.event.verbatimEventDate = "02/02/2"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2002-02-02"){ processed.event.eventDate }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("02"){ processed.event.day }
    expectResult("02"){ processed.event.month }
    expectResult("2002"){ processed.event.year }
  }

}
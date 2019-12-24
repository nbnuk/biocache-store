package au.org.ala.biocache.processor

import java.util.{GregorianCalendar, Date}
import au.org.ala.biocache.parser.{DateParser, EventDate}
import au.org.ala.biocache.model.{QualityAssertion, FullRecord}
import au.org.ala.biocache.util.{DateUtil, StringHelper}
import au.org.ala.biocache.vocab.{AssertionCodes, AssertionStatus, DatePrecision}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Processor for event (date) information.
  */
class EventProcessor extends Processor {

  import AssertionCodes._
  import AssertionStatus._
  import StringHelper._

  val logger = LoggerFactory.getLogger("EventProcessor")

  /**
   * Validate the supplied number using the supplied function.
   */
  def validateNumber(number: String, f: (Int => Boolean)): (Int, Boolean) = {
    try {
      if (number != null) {
        val parsedNumber = number.toInt
        (parsedNumber, f(parsedNumber))
      } else {
        (-1, false)
      }
    } catch {
      case e: NumberFormatException => (-1, false)
    }
  }

  /**
   * Date parsing
   *
   * TODO needs splitting into several methods
   */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]
    if ((raw.event.day == null || raw.event.day.isEmpty)
      && (raw.event.month == null || raw.event.month.isEmpty)
      && (raw.event.year == null || raw.event.year.isEmpty)
      && (raw.event.eventDate == null || raw.event.eventDate.isEmpty)
      && (raw.event.eventDateEnd == null || raw.event.eventDateEnd.isEmpty)
      && (raw.event.verbatimEventDate == null || raw.event.verbatimEventDate.isEmpty)
    ) {
      assertions += QualityAssertion(MISSING_COLLECTION_DATE, "No date information supplied")
//      return assertions.toArray
    }

    var date: Option[java.util.Date] = None
    val currentYear = DateUtil.getCurrentYear
    var comment = ""
    var addPassedInvalidCollectionDate = true
    var dateComplete = false

    var (year, validYear) = validateNumber(raw.event.year, {
      year => year > 0 && year <= currentYear
    })
    var (month, validMonth) = validateNumber(raw.event.month, {
      month => month >= 1 && month <= 12
    })
    var (day, validDay) = validateNumber(raw.event.day, {
      day => day >= 1 && day <= 31
    })

    //check month and day not transposed
    if (!validMonth && raw.event.month.isInt && raw.event.day.isInt) {
      //are day and month transposed?
      val monthValue = raw.event.month.toInt
      val dayValue = raw.event.day.toInt
      if (monthValue > 12 && dayValue <= 12) {
        month = dayValue
        day = monthValue
        assertions += QualityAssertion(DAY_MONTH_TRANSPOSED, "Assume day and month transposed")
        validMonth = true
      } else {
        assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Invalid month supplied")
        addPassedInvalidCollectionDate = false
        assertions += QualityAssertion(DAY_MONTH_TRANSPOSED, PASSED) //this one has been tested and does not apply
      }
    }

    //TODO need to check for other months
    if (day == 0 || day > 31) {
      assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Invalid day supplied")
      addPassedInvalidCollectionDate = false
    }

    //check for sensible year value
    if (year > 0) {
      val (newComment, newValidYear, newYear) = runYearValidation(year, currentYear, day, month)
      comment = newComment
      validYear = newValidYear
      year = newYear

      if (StringUtils.isNotEmpty(comment)) {
        assertions += QualityAssertion(INVALID_COLLECTION_DATE, comment)
        addPassedInvalidCollectionDate = false
      }
    }

    var validDayMonthYear = validYear && validDay && validMonth

    //construct
    if (validDayMonthYear) {
      try {
        val calendar = new GregorianCalendar(
          year.toInt,
          month.toInt - 1,
          day.toInt
        )
        //don't allow the calendar to be lenient we want exceptions with incorrect dates
        calendar.setLenient(false)
        date = Some(calendar.getTime)
        dateComplete = true
      } catch {
        case e: Exception => {
          validDayMonthYear = false
          comment = "Invalid year, day, month"
          assertions += QualityAssertion(INVALID_COLLECTION_DATE, comment)
          addPassedInvalidCollectionDate = false
        }
      }
    }

    //set the processed values
    if (validYear) processed.event.year = year.toString
    if (validMonth) processed.event.month = String.format("%02d", int2Integer(month)) //NC ensure that a month is 2 characters long
    if (validDay) processed.event.day = day.toString
    if (!date.isEmpty) {
      processed.event.eventDate = DateFormatUtils.format(date.get, "yyyy-MM-dd")
    }

    //deal with event date if we don't have separate day, month, year fields
    if (date.isEmpty && raw.event.eventDate != null && !raw.event.eventDate.isEmpty) {
      val parsedDate = DateParser.parseDate(raw.event.eventDate)
      if (!parsedDate.isEmpty) {

        //set our best date
        date = Some(parsedDate.get.parsedStartDate)

        //set processed values
        processed.event.eventDate = parsedDate.get.startDate
        if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
          processed.event.eventDateEnd = parsedDate.get.endDate
        }
        processed.event.day = parsedDate.get.startDay
        processed.event.month = parsedDate.get.startMonth

        //set the valid year if one was supplied in the eventDate
        if(parsedDate.get.startYear != "") {
          val (newComment, isValidYear, newYear) = runYearValidation(
            parsedDate.get.startYear.toInt, currentYear,
            if(parsedDate.get.startDay == "") 0 else parsedDate.get.startDay.toInt,
            if(parsedDate.get.startMonth == "") 0 else parsedDate.get.startMonth.toInt
          )

            comment = newComment
            validYear = isValidYear
            year = newYear

            if(isValidYear) {
              processed.event.year = newYear.toString
              if (processed.event.day != "" && processed.event.month != "") {
                //construct a new event date
                processed.event.eventDate = processed.event.year + "-" + processed.event.month + "-" + processed.event.day
              }
            } else {
              processed.event.eventDate = null
              processed.event.year = null
            }
        }

        if (StringUtils.isNotBlank(parsedDate.get.startDate)) {
          //we have a complete date
          dateComplete = true
        }

        if (DateUtil.isFutureDate(parsedDate.get)) {
          assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Future date supplied")

          addPassedInvalidCollectionDate = false
        }

        if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
          processed.event.eventDateEnd = parsedDate.get.endDate
        }
        setProcessedEventEndDMYvalues(parsedDate,processed)
      } else if (raw.event.eventDate != null && !raw.event.eventDate.isEmpty) {
        //look for an end date
        val parsedDate = DateParser.parseDate(raw.event.eventDate)
        //logger.info("2. date = " + raw.event.eventDate + " parsed = " + parsedDate)
        if (!parsedDate.isEmpty) {
          //what happens if d m y make the eventDate and eventDateEnd is parsed?
          if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
            processed.event.eventDateEnd = parsedDate.get.endDate
          }
          setProcessedEventEndDMYvalues(parsedDate, processed)

          processed.event.day = parsedDate.get.startDay
          processed.event.month = parsedDate.get.startMonth
          processed.event.year = parsedDate.get.startYear
        }
      }
    }

    //process event end date if supplied separately
    if (StringUtils.isNotEmpty(raw.event.eventDateEnd)) {
      //look for an end date
      val parsedDate = DateParser.parseDate(raw.event.eventDateEnd)
      if (!parsedDate.isEmpty&& parsedDate.get.singleDate) { //if not single date then there is definitely a problem with the parsing, e.g. for "09/2012" being interpreted as year range
        //what happens if d m y make the eventDate and eventDateEnd is parsed?
        processed.event.eventDateEnd = parsedDate.get.startDate
        processed.event.endYear = parsedDate.get.startYear
        processed.event.endMonth = parsedDate.get.startMonth
        processed.event.endDay = parsedDate.get.startDay
      }
    }

    //deal with verbatim date if we havent had a parseable eventDate
    if (date.isEmpty && raw.event.verbatimEventDate != null && !raw.event.verbatimEventDate.isEmpty) {
      val parsedDate = DateParser.parseDate(raw.event.verbatimEventDate)
      if (!parsedDate.isEmpty) {
        //set processed values
        processed.event.eventDate = parsedDate.get.startDate
        if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
          processed.event.eventDateEnd = parsedDate.get.endDate
        }
        setProcessedEventEndDMYvalues(parsedDate,processed)

        processed.event.day = parsedDate.get.startDay
        processed.event.month = parsedDate.get.startMonth
        processed.event.year = parsedDate.get.startYear

        //set the valid year if one was supplied in the verbatimEventDate
        if(parsedDate.get.startYear != "") {
          val (newComment, isValidYear, newYear) = runYearValidation(
            parsedDate.get.startYear.toInt,
            currentYear,
            if(parsedDate.get.startDay == "") 0 else parsedDate.get.startDay.toInt,
            if(parsedDate.get.startMonth == "") 0 else parsedDate.get.startMonth.toInt
          )
          comment = newComment
          validYear = isValidYear
          year = newYear

          if(isValidYear) {
            processed.event.year = newYear.toString
            if (processed.event.day != "" && processed.event.month != "") {
              //construct a new event date
              processed.event.eventDate = processed.event.year + "-" + processed.event.month + "-" + processed.event.day
            }
          } else {
            processed.event.eventDate = null
            processed.event.year = null
          }
        }

        //we have a complete date
        dateComplete = StringUtils.isNotBlank(parsedDate.get.startDate)
      }
    } else if ((processed.event.eventDateEnd == null || processed.event.eventDateEnd.isEmpty()) &&
      raw.event.verbatimEventDate != null && !raw.event.verbatimEventDate.isEmpty) {
      //look for an end date
      val parsedDate = DateParser.parseDate(raw.event.verbatimEventDate)
      if (!parsedDate.isEmpty && !parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
        //what happens if d m y make the eventDate and eventDateEnd is parsed?
        if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
          processed.event.eventDateEnd = parsedDate.get.endDate
        }
        setProcessedEventEndDMYvalues(parsedDate,processed)
      }
    }

    //if invalid date, add assertion
    if (!validYear && (processed.event.eventDate == null || processed.event.eventDate == "" || comment != "")) {
      assertions += QualityAssertion(INVALID_COLLECTION_DATE, comment)
      addPassedInvalidCollectionDate = false
    }

    //check for future date
    if (!date.isEmpty && date.get.after(new Date())) {
      assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Future date supplied")
      addPassedInvalidCollectionDate = false
    }

    //check to see if we need add a passed test for the invalid collection dates
    if (addPassedInvalidCollectionDate) {
      assertions += QualityAssertion(INVALID_COLLECTION_DATE, PASSED)
    }

    //indicate if we have complete date information - defined as a single day date
    if (dateComplete) {
      //add a pass condition for this test
      assertions += QualityAssertion(INCOMPLETE_COLLECTION_DATE, PASSED)
    } else {
      //incomplete date
      assertions += QualityAssertion(INCOMPLETE_COLLECTION_DATE, "The supplied collection date is not complete")
    }

    //now process the other dates
    processOtherDates(raw, processed, assertions)

    //check for the "first" of month,year,century
    processFirstDates(raw, processed, assertions)

    //validate against date precision
    checkPrecision(raw, processed, assertions)

    //smp+
    // If we have a date range, then check that the end date is not before the start date;
    // raising a data quality assertion if it is.

    if ((processed.event.datePrecision == DAY_RANGE_PRECISION) ||
        (processed.event.datePrecision == MONTH_RANGE_PRECISION) ||
        (processed.event.datePrecision == YEAR_RANGE_PRECISION)) {

      // note: depending on the precision not all event fields are populated
      def toIntOr1(s: String): Int = {
        try {
          s.toInt
        } catch {
          case e: Exception => 1
        }
      }

      val rangeStartDate = new Date( toIntOr1(processed.event.year) - 1900, toIntOr1(processed.event.month) - 1, toIntOr1(processed.event.day) )
      val rangeEndDate = new Date( toIntOr1(processed.event.endYear) - 1900, toIntOr1(processed.event.endMonth) - 1, toIntOr1(processed.event.endDay) )

      if (rangeStartDate.after( rangeEndDate )) {
        assertions += QualityAssertion( INVALID_COLLECTION_DATE, "End date is before start date" )
      }
    }
    //smp-

    assertions.toArray
  }

  //set event end day+month+year values if not equal to starting day+month+year
  def setProcessedEventEndDMYvalues(parsedDate: Option[EventDate], processed: FullRecord) = {
    if (!parsedDate.get.endYear.equals(parsedDate.get.startYear) ||
      !parsedDate.get.endMonth.equals(parsedDate.get.startMonth) ||
      !parsedDate.get.endDay.equals(parsedDate.get.startDay)) {
      processed.event.endYear = parsedDate.get.endYear
      processed.event.endMonth = parsedDate.get.endMonth
      processed.event.endDay = parsedDate.get.endDay
    }
  }


  /**
    * Validate the supplied year.
    *
    * @param rawyear
    * @param currentYear
    * @param day
    * @param month
    * @return
    */
  def runYearValidation(rawyear: Int, currentYear: Int, day: Int = 0, month: Int = 0): (String, Boolean, Int) = {
    var validYear = true
    var comment = ""
    var year = rawyear
    if (year > 0) {
      if (year < 100) {
        //parse 89 for 1989
        if (year > currentYear % 100) {
          // Must be in last century
          year += ((currentYear / 100) - 1) * 100
        } else {
          // Must be in this century
          year += (currentYear / 100) * 100

          //although check that combined year-month-day isnt in the future
          if (day != 0 && month != 0) {
            val date = DateUtils.parseDate(year.toString + String.format("%02d", int2Integer(month)) + day.toString, Array("yyyyMMdd"))
            if (date.after(new Date())) {
              year -= 100
            }
          }
        }
      } else if (year >= 100 && year < 1600) {
        year = -1
        validYear = false
        comment = "Year out of range"
      } else if (year > DateUtil.getCurrentYear) {
        year = -1
        validYear = false
        comment = "Future year supplied"
        //assertions + QualityAssertion(INVALID_COLLECTION_DATE,comment)
      } else if (year == 1788 && month == 1 && day == 26) {
        //First fleet arrival date indicative of a null date.
        validYear = false
        comment = "First Fleet arrival implies a null date"
      }

    }
    (comment, validYear, year)
  }

  def processFirstDates(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {
    //check to see if the date is the first of a month
    if (processed.event.day == "1" || processed.event.day == "01") {
      assertions += QualityAssertion(FIRST_OF_MONTH)
      //check to see if the date is the first of the year
      if (processed.event.month == "01" || processed.event.month == "1") {
        assertions += QualityAssertion(FIRST_OF_YEAR)
        //check to see if the date is the first of the century
        if (processed.event.year != null) {
          val (year, validYear) = validateNumber(processed.event.year, {
            year => year > 0
          })
          if (validYear && year % 100 == 0) {
            assertions += QualityAssertion(FIRST_OF_CENTURY)
          } else {
            //the date is NOT the first of the century
            assertions += QualityAssertion(FIRST_OF_CENTURY, PASSED)
          }
        }
      } else if (processed.event.month != null) {
        //the date is not the first of the year
        assertions += QualityAssertion(FIRST_OF_YEAR, PASSED)
      }
    } else if (processed.event.day != null) {
      //the date is not the first of the month
      assertions += QualityAssertion(FIRST_OF_MONTH, PASSED)
    }
  }

  /**
   * processed the other dates for the occurrence including performing data checks
    *
    * @param raw
   * @param processed
   * @param assertions
   */
  def processOtherDates(raw:FullRecord, processed:FullRecord, assertions:ArrayBuffer[QualityAssertion]){
    //process the "modified" date for the occurrence - we want all modified dates in the same format so that we can index on them...
    if (raw.occurrence.modified != null) {
      val parsedDate = DateParser.parseDate(raw.occurrence.modified)
      if (parsedDate.isDefined) {
        processed.occurrence.modified = parsedDate.get.startDate
      }
    }

    if (raw.identification.dateIdentified != null) {
      val parsedDate = DateParser.parseDate(raw.identification.dateIdentified)
      if (parsedDate.isDefined)
        processed.identification.dateIdentified = parsedDate.get.startDate
    }

    if (raw.location.georeferencedDate != null || raw.miscProperties.containsKey("georeferencedDate")){
      def rawdate = if (raw.location.georeferencedDate != null) {
        raw.location.georeferencedDate
      } else {
        raw.miscProperties.get("georeferencedDate")
      }
      val parsedDate = DateParser.parseDate(rawdate)
      if (parsedDate.isDefined) {
        processed.location.georeferencedDate = parsedDate.get.startDate
      }
    }

    if (StringUtils.isNotBlank(processed.event.eventDate)) {
      val eventDate = DateParser.parseStringToDate(processed.event.eventDate)
      if(eventDate.isDefined) {
        //now test if the record was identified before it was collected
        if (StringUtils.isNotBlank(processed.identification.dateIdentified)) {
          if (DateParser.parseStringToDate(processed.identification.dateIdentified).get.before(eventDate.get)) {
            //the record was identified before it was collected !!
            assertions += QualityAssertion(ID_PRE_OCCURRENCE, "The records was identified before it was collected")
          } else {
            assertions += QualityAssertion(ID_PRE_OCCURRENCE, PASSED)
          }
        }

        //now check if the record was georeferenced after the collection date
        if (StringUtils.isNotBlank(processed.location.georeferencedDate)) {
          if (DateParser.parseStringToDate(processed.location.georeferencedDate).get.after(eventDate.get)) {
            //the record was not georeference when it was collected!!
            assertions += QualityAssertion(GEOREFERENCE_POST_OCCURRENCE, "The record was not georeferenced when it was collected")
          } else {
            assertions += QualityAssertion(GEOREFERENCE_POST_OCCURRENCE, PASSED)
          }
        }
      }
    }
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]

    //get the data resource information to check if it has mapped collections
    if (lastProcessed.isDefined) {
      assertions ++= lastProcessed.get.findAssertions(Array(GEOREFERENCE_POST_OCCURRENCE.code, ID_PRE_OCCURRENCE.code,
        MISSING_COLLECTION_DATE.code, DAY_MONTH_TRANSPOSED.code, INVALID_COLLECTION_DATE.code,
        INCOMPLETE_COLLECTION_DATE.code, FIRST_OF_MONTH.code, FIRST_OF_YEAR.code, FIRST_OF_CENTURY.code))

      //update the details from lastProcessed
      processed.event = lastProcessed.get.event
      processed.occurrence.modified = lastProcessed.get.occurrence.modified
      processed.identification.dateIdentified = lastProcessed.get.identification.dateIdentified
      processed.location.georeferencedDate = lastProcessed.get.location.georeferencedDate
    }

    assertions.toArray
  }


  /**
    * Check the precision of the supplied date and alter the processed date
    *
    * @param raw
    * @param processed
    * @param assertions
    */
  def checkPrecision(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {

    if(StringUtils.isNotBlank(raw.event.datePrecision) && StringUtils.isNotBlank(processed.event.eventDate)) {
      val matchedTerm = DatePrecision.matchTerm(raw.event.datePrecision)
      if (!matchedTerm.isEmpty) {
        val term = matchedTerm.get
        processed.event.datePrecision = term.canonical

        if (term.canonical.equalsIgnoreCase(DAY_PRECISION)){
          //is the processed date in yyyy-MM format
          reformatToPrecision(processed, "yyyy-MM-dd", false, false, false)
        }
        else if (term.canonical.equalsIgnoreCase(MONTH_PRECISION)){
          //is the processed date in yyyy-MM format
          reformatToPrecision(processed, "yyyy-MM", true, false, false)
        }
        else if (term.canonical.equalsIgnoreCase(YEAR_PRECISION)){
          //is the processed date in yyyy format
          reformatToPrecision(processed, "yyyy", true, true, false)
        }
        else if (term.canonical.equalsIgnoreCase(DAY_RANGE_PRECISION)){
          //is the processed date in yyyy-MM format
          reformatToPrecision(processed, "yyyy-MM-dd", false, false, false)
        }
        else if (term.canonical.equalsIgnoreCase(MONTH_RANGE_PRECISION)){
          //is the processed date in yyyy-MM format
          reformatToPrecision(processed, "yyyy-MM", true, false, false)
        }
        else if (term.canonical.equalsIgnoreCase(YEAR_RANGE_PRECISION)){
          //is the processed date in yyyy format
          reformatToPrecision(processed, "yyyy", true, true, false) //don't blank year, since end year will be populated
        }
        else {
          reformatToPrecision(processed, "yyyy-MM-dd", false, false, false)
        }
      }
    } else {
      reformatToPrecision(processed, "yyyy-MM-dd", false, false, false)
    }
  }


  /**
    * TODO handle all the permutations of year and month ranges.
    *
    * @param processed
    * @param format date format to use
    * @param forceNullifyDay nullify single day value for any value
    * @param forceNullifyMonth nullify single month value for any value
    * @param forceNullifyYear nullify single year value for any value
    */
  def reformatToPrecision(processed:FullRecord, format:String, forceNullifyDay:Boolean, forceNullifyMonth:Boolean, forceNullifyYear:Boolean): Unit = {

    if (processed.event.day == null || processed.event.day.isEmpty) processed.event.day = null
    if (processed.event.month == null || processed.event.month.isEmpty) processed.event.month = null
    if (processed.event.year == null || processed.event.year.isEmpty) processed.event.year = null
    if (processed.event.endDay == null || processed.event.endDay.isEmpty) processed.event.endDay = null
    if (processed.event.endMonth == null || processed.event.endMonth.isEmpty) processed.event.endMonth = null
    if (processed.event.endYear == null || processed.event.endYear.isEmpty) processed.event.endYear = null

    val startDate = DateParser.parseDate(processed.event.eventDate)
    val endDate = DateParser.parseDate(processed.event.eventDateEnd)

    if(!startDate.isEmpty) {

      if (startDate.get.singleDate && startDate.get.parsedStartDate != null) {
        try {
          processed.event.eventDate = DateFormatUtils.format(startDate.get.parsedStartDate, format)
        } catch {
          case e: Exception => logger.error("Problem reformatting date to new precision")
        }
      }
    }

    if(!endDate.isEmpty) {

      if (endDate.get.singleDate && endDate.get.parsedStartDate != null) {
        try {
          processed.event.eventDateEnd = DateFormatUtils.format(endDate.get.parsedStartDate, format)
        } catch {
          case e: Exception => logger.error("Problem reformatting date to new precision")
        }
      }
    }

    //single date
    if (forceNullifyDay) {
      processed.event.day = null
      processed.event.endDay = null
    }
    if (forceNullifyMonth) {
      processed.event.month = null
      processed.event.endMonth = null
    }
    if (forceNullifyYear) {
      processed.event.year = null
      processed.event.endYear = null
    }

    var determinedDatePrecision = ""

    /* Surely this should depend on precision? */
    /*
    if(!startDate.isEmpty && !endDate.isEmpty) {

      //ranges - nullify if not equal
      if (StringUtils.isNotEmpty(startDate.get.startDay) && StringUtils.isNotEmpty(endDate.get.startDay) && startDate.get.startDay != endDate.get.startDay) {
        processed.event.day = null
      }
      if (StringUtils.isNotEmpty(startDate.get.startMonth) && StringUtils.isNotEmpty(endDate.get.startMonth) && startDate.get.startMonth != endDate.get.startMonth) {
        processed.event.month = null
        processed.event.day = null
      }
      if (StringUtils.isNotEmpty(startDate.get.startYear) && StringUtils.isNotEmpty(endDate.get.startYear) && startDate.get.startYear != endDate.get.startYear) {
        processed.event.year = null
        processed.event.month = null //of the year is different, and its a range, month cant be determined
        processed.event.day = null
      }
    }
    */

    // attempt to calculate a date precision based on the values
    if (StringUtils.isEmpty(processed.event.datePrecision)){
      //before date (NBN)
      if ((processed.event.day == null && processed.event.month == null && processed.event.year == null) &&
        (processed.event.endDay != null || processed.event.endMonth != null || processed.event.endYear != null)) {
        if (processed.event.endDay != null) {
          determinedDatePrecision = BEFORE_DAY_PRECISION
        } else if (processed.event.endMonth != null) {
          determinedDatePrecision = BEFORE_MONTH_PRECISION
        } else {
          determinedDatePrecision = BEFORE_YEAR_PRECISION
        }
      }
      //do we have a range
      else
      if(!startDate.isEmpty && !endDate.isEmpty) {
        determinedDatePrecision = DAY_RANGE_PRECISION
        //can only be day range precision because dates only populated if complete (though a defined date precision might reduce them, but then this would not be called)
      } else if (processed.event.endDay != null ||
        processed.event.endMonth != null ||
        processed.event.endYear != null) {
        //ignore raw enddate if it was unparseable into a processed end date or end year/month/day
        determinedDatePrecision = DAY_RANGE_PRECISION //assume day range precision, then downgrade as required
        if (processed.event.endDay == null) {
          determinedDatePrecision = MONTH_RANGE_PRECISION
        }
        if (processed.event.endMonth == null) {
          determinedDatePrecision = YEAR_RANGE_PRECISION
        }
        if (processed.event.endYear == null) {
          determinedDatePrecision = NOT_SUPPLIED
        }
        if (((processed.event.day == null) != (processed.event.endDay == null)) ||
          ((processed.event.month == null) != (processed.event.endMonth == null)) ||
          ((processed.event.year == null) != (processed.event.endYear == null))){
          determinedDatePrecision = NOT_SUPPLIED //mismatched precisions because day/month/year specified for start or end date but not the other, e.g. startdate = 2000-02-03 and enddate = 2000-04
        }
      } else if (!startDate.isEmpty) {
        determinedDatePrecision = DAY_PRECISION
        //single date
      } else if (processed.event.day == null && processed.event.month != null && processed.event.year != null) {
        determinedDatePrecision = MONTH_PRECISION
      } else if (processed.event.day == null && processed.event.month == null && processed.event.year != null) {
        determinedDatePrecision = YEAR_PRECISION
      } else {
        determinedDatePrecision = NOT_SUPPLIED
      }

      processed.event.datePrecision = determinedDatePrecision
    }
  }

  val DAY_RANGE_PRECISION = "Day Range"
  val MONTH_RANGE_PRECISION = "Month Range"
  val YEAR_RANGE_PRECISION = "Year Range"
  val NOT_SUPPLIED = "Not Supplied"

  val DAY_PRECISION = "Day"
  val MONTH_PRECISION = "Month"
  val YEAR_PRECISION = "Year"

    val BEFORE_DAY_PRECISION = "Before Day"
    val BEFORE_MONTH_PRECISION = "Before Month"
    val BEFORE_YEAR_PRECISION = "Before Year"

  def getName = "event"
}

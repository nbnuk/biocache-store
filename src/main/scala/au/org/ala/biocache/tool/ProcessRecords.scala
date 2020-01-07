package au.org.ala.biocache.tool

import au.org.ala.biocache._
import java.io.File
import org.slf4j.LoggerFactory
import au.org.ala.biocache.util.{StringConsumer, OptionParser, FileHelper}
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}
import au.org.ala.biocache.processor.RecordProcessor
import java.util.concurrent.ArrayBlockingQueue

/**
 * A simple threaded implementation of the processing.
 */
object ProcessRecords extends Tool with IncrementalTool {

  import FileHelper._

  def cmd = "process"
  def desc = "Process records (geospatial, taxonomy)"

  val occurrenceDAO = Config.occurrenceDAO
  val persistenceManager = Config.persistenceManager
  val logger = LoggerFactory.getLogger("ProcessRecords")

  def main(args : Array[String]) : Unit = {

    logger.info("Starting processing...")
    var threads:Int = 4
    var checkDeleted = false
    var dataResourceUid:Option[String] = None
    var checkRowKeyFile = true
    var rowKeyFile = ""
    var abortIfNotRowKeyFile = true

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("dr", "resource", "The data resource to process", {v:String => dataResourceUid = Some(v)})
      opt("rf", "file-rowkeys-to-index", "Absolute file path to fle containing rowkeys to index", { v: String => rowKeyFile = v })
      booleanOpt("cd", "checkDeleted", "Check deleted records", {v:Boolean => checkDeleted = v})
    }

    if(parser.parse(args)){

      if(!dataResourceUid.isEmpty && checkRowKeyFile){
        val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid.get)
        rowKeyFile = retrievedRowKeyFile.getOrElse("")
      }

      if(abortIfNotRowKeyFile && (rowKeyFile == "" || !(new File(rowKeyFile).exists()))){
        logger.warn("No rowkey file was found for this processing. Aborting.")
      } else {
        if (rowKeyFile != "") {
          //process the row key file
          processFileOfRowKeys(new java.io.File(rowKeyFile), threads)
        } else {
          logger.info("rowkey file not found")
        }
      }
    }
  }

  def processRecords(dataResourceUid:String, threads: Int): Unit = {

    val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid)

    if(retrievedRowKeyFile.isEmpty || !new File(retrievedRowKeyFile.get).exists()){
      logger.warn("No rowkey file was found for this processing. Aborting.")
    } else {
        //process the row key file
        processFileOfRowKeys(new java.io.File(retrievedRowKeyFile.get), threads)
    }
  }

  /**
    * Process a set of records with keys in the supplied file
    * @param rowkeys
    */
  def processRowKeys(rowkeys:List[String]) {

    val queue = rowkeys
    var ids = 0
    var counter = 0
    val recordProcessor = new RecordProcessor
    rowkeys.foreach { guid =>
      counter += 1
      val rawProcessed = Config.occurrenceDAO.getRawProcessedByRowKey(guid)
      if (!rawProcessed.isEmpty) {
        val rp = rawProcessed.get
        recordProcessor.processRecord(rp(0), rp(1))
      }
    }

    logger.info("Total records processed: " + counter)
  }

  /**
   * Process a set of records with keys in the supplied file
   * @param file
   * @param threads
   */
  def processFileOfRowKeys(file: java.io.File, threads: Int) {
    val queue = new ArrayBlockingQueue[String](100)
    var ids = 0
    var counter = 0
    val recordProcessor = new RecordProcessor
    var slowDownCount = 0                       // accumulated count of slow down detected
    val slowDownMax = 5                         // maximum number of accumulated slow downs before we abort
    val rpsMin = 150f                           // records per second processing limit under which we consider we are running too slowly
    var cleanAbort = false                      // flag to indicate we are aborting because we are running too slowly

    val pool: Array[StringConsumer] = Array.fill(threads) {

      var startTime = System.currentTimeMillis
      var finishTime = System.currentTimeMillis

      val p = new StringConsumer(queue, ids, { guid =>
        counter += 1
        val rawProcessed = Config.occurrenceDAO.getRawProcessedByRowKey(guid)

        if (!rawProcessed.isEmpty) {
          val rp = rawProcessed.get
          recordProcessor.processRecord(rp(0), rp(1))

          //debug counter
          if (counter % 1000 == 0) {
            finishTime = System.currentTimeMillis

            // calculate records per second
            val rps = 1000f / (((finishTime - startTime).toFloat) / 1000f)

            // let the poor user know the run rate
            logger.info(counter + " >> Last key : " + rp(0).rowKey + ", records per sec: " + rps)

            // if we aren't already aborting because we are running too slow then check to see if we should abort
            if (!cleanAbort) {
              // are we running too slow?
              if (rps < rpsMin) {
                // accumulate, as we don't want one 'blip' to cause us to abort
                slowDownCount = slowDownCount + 1
                // let the poor user know what's happening
                logger.info("slow down detected: (" + slowDownCount + "/" + slowDownMax + ")")
                // should we abort now?
                if (slowDownCount >= slowDownMax) {
                  // yes, so flag it
                  cleanAbort = true
                  logger.info("slow down detected: accumulated speed is too slow, aborting")
                  // We want to exit this worker thread now.
                  // this code runs on the context of StringConsumer.run (we are in a closure here)
                  // we can't access instance variables of StringConsumer here so instead to abort the
                  // while loop we are running in within StringConsumer.run we have to throw an
                  // exception. See StringConsumer.run and hopefully this will become clear.
                  throw new RuntimeException("too slow, aborting")
                }
              }
              else {
                // reset to avoid 'blips', we need at least slowDownMax consecutive counts of
                // slow rps in order to abort
                slowDownCount = 0
              }
            }

            startTime = System.currentTimeMillis
          }
        }
      })
      ids += 1
      p.start
      p
    }

    // file.foreachLine(line => { if (slowDownCount <= 2) { queue.put(line.trim) } })

    // queue is a blocking queue, it will block on attempt to add an item to an already
    // full queue. This gives side effect that our program will never exit if we have
    // a task blocked waiting to add to this queue (the foreground task here), when we have
    // aborted all the worker threads due to slow running.
    //
    // The code below checks for aborts and does not now block on a few queue, but still
    // retain original intent and behaviour for the non-aborting case.

    file.foreachLine( line => {
      if (!cleanAbort) {
        var retry = false
        do {
          retry = !queue.offer(line, 1, java.util.concurrent.TimeUnit.SECONDS)
          if (cleanAbort) {
            retry = false
          }
        } while (retry)
      }
    })

    // wait and join as normal

    pool.foreach(t => t.shouldStop = true)
    pool.foreach(_.join)

    logger.info("Total records processed: " + counter)
  }
}
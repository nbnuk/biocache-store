package au.org.ala.biocache

import au.org.ala.biocache.dao.{DeletedRecordDAO, DeletedRecordDAOImpl, DuplicateDAO, DuplicateDAOImpl, OccurrenceDAO, OccurrenceDAOImpl, OutlierStatsDAO, OutlierStatsDAOImpl, QidDAO, QidDAOImpl, ValidationRuleDAO, ValidationRuleDAOImpl}
import au.org.ala.biocache.index.{IndexDAO, SolrIndexDAO}
import au.org.ala.biocache.persistence.{Cassandra3PersistenceManager, MockPersistenceManager, PersistenceManager}
import au.org.ala.names.search.ALANameSearcher
import com.google.inject.{AbstractModule, Scopes}
import com.google.inject.name.Names
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.Properties

/**
 * Guice configuration module.
 */
class ConfigModule extends AbstractModule {

  protected val logger = LoggerFactory.getLogger("ConfigModule")

  val properties = {

    val properties = new Properties()
    //NC 2013-08-16: Supply the properties file as a system property via -Dbiocache.config=<file>
    //or the default /data/biocache/config/biocache-test-config.properties file is used.

    //check to see if a system property has been supplied with the location of the config file
    val filename = System.getProperty("biocache.config", "/data/biocache/config/biocache-config.properties")
    logger.info("Using config file: " + filename)

    val file = new java.io.File(filename)

    //only load the properties file if it exists otherwise default to the biocache-test-config.properties on the classpath
    val stream = if(file.exists()) {
      new FileInputStream(file)
    } else {
      this.getClass.getResourceAsStream(filename)
    }

    if(stream == null){
      throw new RuntimeException("Configuration file not found. Please add to classpath or /data/biocache/config/biocache-config.properties")
    }

    logger.debug("Loading configuration from " + filename)
    properties.load(stream)

    //this allows the SDS to access the same config file
    System.setProperty("sds.config.file", filename)

    properties
  }

  override def configure() {

    Names.bindProperties(this.binder, properties)
    //bind concrete implementations
    logger.debug("Initialising DAOs")
    bind(classOf[OccurrenceDAO]).to(classOf[OccurrenceDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[OutlierStatsDAO]).to(classOf[OutlierStatsDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[DeletedRecordDAO]).to(classOf[DeletedRecordDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[DuplicateDAO]).to(classOf[DuplicateDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[ValidationRuleDAO]).to(classOf[ValidationRuleDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[QidDAO]).to(classOf[QidDAOImpl]).in(Scopes.SINGLETON)
    logger.debug("Initialising SOLR")
    bind(classOf[IndexDAO]).to(classOf[SolrIndexDAO]).in(Scopes.SINGLETON)
    logger.debug("Initialising name matching indexes")
    try {
      val nameIndexLocation = properties.getProperty("name.index.dir")
      logger.debug("Loading name index from " + nameIndexLocation)
      val nameIndex = new ALANameSearcher(nameIndexLocation)
      bind(classOf[ALANameSearcher]).toInstance(nameIndex)
    } catch {
      case e: Exception => logger.warn("Lucene indexes are not currently available. " +
        "Please check 'name.index.dir' property in config. Message: " + e.getMessage())
    }
    logger.debug("Initialising persistence manager")
    properties.getProperty("db") match {
      case "mock" => bind(classOf[PersistenceManager]).to(classOf[MockPersistenceManager]).in(Scopes.SINGLETON)
      //      case "cassandra" => bind(classOf[PersistenceManager]).to(classOf[CassandraPersistenceManager]).in(Scopes.SINGLETON)
      case "cassandra3" => bind(classOf[PersistenceManager]).to(classOf[Cassandra3PersistenceManager]).in(Scopes.SINGLETON)
      case _ => throw new RuntimeException("Persistence manager type unrecognised. Please check your external config file. ")
    }
    logger.debug("Configure complete")
  }
}

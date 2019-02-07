package au.org.ala.biocache.vocab

/**
  * Vocabulary matcher for License values.
  */
object License extends Vocab {
  val all = loadVocabFromVerticalFile("/license.txt")
}
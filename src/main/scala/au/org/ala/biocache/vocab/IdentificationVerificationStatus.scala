package au.org.ala.biocache.vocab

/**
  * Vocabulary matcher for Identification verification status values.
  */
object IdentificationVerificationStatus extends Vocab {
  val all = loadVocabFromVerticalFile("/identificationVerificationStatus.txt")
}


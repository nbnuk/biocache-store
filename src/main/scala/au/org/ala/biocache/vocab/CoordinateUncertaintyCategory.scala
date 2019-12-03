package au.org.ala.biocache.vocab

object CoordinateUncertaintyCategory {
  def getCategory(string2Match: String) : String = {

    var coordinateUncertaintyCategory = "Unknown"
    if (string2Match != null && string2Match != "") {
      coordinateUncertaintyCategory = string2Match.toDouble match {
        case x if x <= 1      => "<=1m"
        case x if x <= 10     => "1 - 10m"
        case x if x <= 100    => "10 - 100m"
        case x if x <= 1000   => "100 - 1000m"
        case x if x <= 2000   => "1000 - 2000m"
        case x if x <= 5000   => "2 - 5km"
        case x if x <= 10000  => "5 - 10km"
        case x if x <= 20000  => "10 - 20km"
        case x if x <= 50000  => "20 - 50km"
        case x if x <= 100000 => "50 - 100km"
        case x if x > 100000  => ">100km"
        case _                => "Unknown"
      }
    }
    coordinateUncertaintyCategory
  }

}

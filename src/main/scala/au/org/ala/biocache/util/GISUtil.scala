package au.org.ala.biocache.util

import org.apache.commons.math3.util.Precision
import org.geotools.geometry.GeneralDirectPosition
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory

/**
  * GIS Utilities.
  */
object GISUtil {

  val WGS84_EPSG_Code = "EPSG:4326"

  /**
    * Re-projects coordinates into WGS 84
    *
    * @param coordinate1 first coordinate. If source value is easting/northing, then this should be the easting value.
    *                    Otherwise it should be the latitude
    * @param coordinate2 first coordinate. If source value is easting/northing, then this should be the northing value.
    *                    Otherwise it should be the longitude
    * @param sourceCrsEpsgCode epsg code for the source CRS, e.g. EPSG:4202 for AGD66
    * @param decimalPlacesToRoundTo number of decimal places to round the reprojected coordinates to
    * @return Reprojected coordinates (latitude, longitude), or None if the operation failed.
    */
  def reprojectCoordinatesToWGS84(coordinate1: Double, coordinate2: Double, sourceCrsEpsgCode: String,
                                  decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    try {
      val wgs84CRS = DefaultGeographicCRS.WGS84
      val sourceCRS = CRS.decode(sourceCrsEpsgCode)
      val transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, wgs84CRS)
      val directPosition = new GeneralDirectPosition(coordinate1, coordinate2)
      val wgs84LatLong = transformOp.getMathTransform().transform(directPosition, null)

      //NOTE - returned coordinates are longitude, latitude, despite the fact that if
      //converting latitude and longitude values, they must be supplied as latitude, longitude.
      //No idea why this is the case.
      val longitude = wgs84LatLong.getOrdinate(0)
      val latitude = wgs84LatLong.getOrdinate(1)

      val roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo)
      val roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo)

      Some(roundedLatitude.toString, roundedLongitude.toString)
    } catch {
      case ex: Exception => None
    }
  }

  def reprojectCoordinatesWGS84ToOSGB36(coordinate1: Double, coordinate2: Double,
                                  decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    try {
      val wgs84CRS = CRS.decode("EPSG:4326", false)
      val osgb36CRS = CRS.decode("EPSG:27700", false)

      val transformOp = new DefaultCoordinateOperationFactory().createOperation(wgs84CRS, osgb36CRS)
      val directPosition = new GeneralDirectPosition(coordinate1, coordinate2)
      val osgb36LatLong = transformOp.getMathTransform().transform(directPosition, null)

      val longitude = osgb36LatLong.getOrdinate(0)
      val latitude = osgb36LatLong.getOrdinate(1)

      val roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo)
      val roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo)

      Some(roundedLatitude.toString, roundedLongitude.toString)
    } catch {
      case ex: Exception => None
    }
  }

  /**
    * This is a port of this javascript code:
   *
  * http://www.movable-type.co.uk/scripts/latlong-gridref.html (i.e. https://cdn.rawgit.com/chrisveness/geodesy/v1.1.3/osgridref.js)
  */

  def coordinatesOSGB36toNorthingEasting(lat: Double, lon: Double, decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    //TODO: it would be good if this came from a library instead of written the hard (and potentially buggy) way.
    try {
      val φ = lat.toRadians
      val λ = lon.toRadians

      val a = 6377563.396
      val b = 6356256.909 // Airy 1830 major & minor semi-axes
      val F0 = 0.9996012717 // NatGrid scale factor on central meridian
      val φ0 = (49).toRadians
      val λ0 = (-2).toRadians // NatGrid true origin is 49°N,2°W
      val N0 = -100000
      val E0 = 400000 // northing & easting of true origin, metres
      val e2 = 1 - (b * b) / (a * a) // eccentricity squared
      val n = (a - b) / (a + b)
      val n2 = n * n
      val n3 = n * n * n; // n, n², n³

      val cosφ = Math.cos(φ)
      val sinφ = Math.sin(φ)
      val ν = a * F0 / Math.sqrt(1 - e2 * sinφ * sinφ) // nu = transverse radius of curvature
      val ρ = a * F0 * (1 - e2) / Math.pow(1 - e2 * sinφ * sinφ, 1.5) // rho = meridional radius of curvature
      val η2 = ν / ρ - 1 // eta = ?

      val Ma = (1 + n + (5 / 4) * n2 + (5 / 4) * n3) * (φ - φ0)
      val Mb = (3 * n + 3 * n * n + (21 / 8) * n3) * Math.sin(φ - φ0) * Math.cos(φ + φ0)
      val Mc = ((15 / 8) * n2 + (15 / 8) * n3) * Math.sin(2 * (φ - φ0)) * Math.cos(2 * (φ + φ0))
      val Md = (35 / 24) * n3 * Math.sin(3 * (φ - φ0)) * Math.cos(3 * (φ + φ0))
      val M = b * F0 * (Ma - Mb + Mc - Md) // meridional arc

      val cos3φ = cosφ * cosφ * cosφ
      val cos5φ = cos3φ * cosφ * cosφ
      val tan2φ = Math.tan(φ) * Math.tan(φ)
      val tan4φ = tan2φ * tan2φ

      val I = M + N0
      val II = (ν / 2) * sinφ * cosφ
      val III = (ν / 24) * sinφ * cos3φ * (5 - tan2φ + 9 * η2)
      val IIIA = (ν / 720) * sinφ * cos5φ * (61 - 58 * tan2φ + tan4φ)
      val IV = ν * cosφ
      val V = (ν / 6) * cos3φ * (ν / ρ - tan2φ)
      val VI = (ν / 120) * cos5φ * (5 - 18 * tan2φ + tan4φ + 14 * η2 - 58 * tan2φ * η2)

      val Δλ = λ - λ0
      val Δλ2 = Δλ * Δλ
      val Δλ3 = Δλ2 * Δλ
      val Δλ4 = Δλ3 * Δλ
      val Δλ5 = Δλ4 * Δλ
      val Δλ6 = Δλ5 * Δλ

      var N = I + II * Δλ2 + III * Δλ4 + IIIA * Δλ6
      var E = E0 + IV * Δλ + V * Δλ3 + VI * Δλ5

      N = Precision.round(N, decimalPlacesToRoundTo) //(3 decimals = mm precision)
      E = Precision.round(E, decimalPlacesToRoundTo)
      Some(N.toString, E.toString)
    } catch {
      case ex: Exception => None
    }
  }
}

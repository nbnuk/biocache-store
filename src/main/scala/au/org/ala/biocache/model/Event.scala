package au.org.ala.biocache.model

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}

import scala.beans.BeanProperty
import org.apache.commons.lang.builder.ToStringBuilder
import au.org.ala.biocache.poso.POSO

/**
 * POSO for holding event data for an occurrence
 */
@JsonIgnoreProperties(Array("propertyNames"))
class Event extends Cloneable with POSO {
  override def clone : Event = super.clone.asInstanceOf[Event]
  @BeanProperty var day:String = _
  @BeanProperty var endDayOfYear:String = _
  @BeanProperty var eventAttributes:String = _
  @BeanProperty var eventDate:String = _
  @BeanProperty var eventDateEnd:String = _
  @BeanProperty var eventID:String = _
  @BeanProperty var eventRemarks:String = _
  @BeanProperty var eventTime:String = _
  @BeanProperty var verbatimEventDate:String = _
  @BeanProperty var year:String = _
  @BeanProperty var month:String = _
  @BeanProperty var startDayOfYear:String = _
  //custom date range fields
  @BeanProperty var startYear:String = _
  @BeanProperty var endYear:String = _
  @BeanProperty var endMonth:String = _
  @BeanProperty var endDay:String = _
  @BeanProperty var datePrecision:String = _

  override def toString = ToStringBuilder.reflectionToString(this)
}

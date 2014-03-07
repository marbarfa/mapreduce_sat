package scala.domain

/**
 * Created by marbarfa on 3/2/14.
 */
class Variable(val number : Integer) {

  var boolValue : Boolean = null.asInstanceOf[Boolean]

  def isValueSet : Boolean = boolValue != null.asInstanceOf[Boolean]
}

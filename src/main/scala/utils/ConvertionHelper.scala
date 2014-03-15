package main.scala.utils

/**
 * Created by marbarfa on 3/9/14.
 */
trait ConvertionHelper {

  implicit def bool2int(b: Boolean) : Int = if (b) 1 else 0;
  implicit def int2bool(b: Int) : Boolean = if (b==1) true else false;
}

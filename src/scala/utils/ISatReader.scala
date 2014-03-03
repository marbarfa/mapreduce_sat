package scala.utils

import scala.domain.Formula

/**
 * Interface of the 3SAT instance reader.s
 *
 * Created by marbarfa on 3/2/14.
 */
trait ISatReader {

  abstract def read3SatInstance(intance_path : String) : Formula;

}

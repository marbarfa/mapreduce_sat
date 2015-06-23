package utils

import common.SatMapReduceHelper
import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
 * Created by mbarreto on 6/21/15.
 */
class SatMapReduceHelperTest extends FlatSpec with Matchers with BeforeAndAfter {


 "An int" should "return the correct binary format" in {
   SatMapReduceHelper.toBinary(1, 5) should be ("00001")
   SatMapReduceHelper.toBinary(4, 5) should be ("00100")
   SatMapReduceHelper.toBinary(4, 3) should be ("100")

   SatMapReduceHelper.toBinary(10, 3) should be ("010")
 }

  "A literal map" should "be mapped based on its binary representation" in {
    SatMapReduceHelper.createMap(List(1, 2, 3), "001") should be (List(-1,-2,3))
    SatMapReduceHelper.createMap(List(1, 2, 3, -4), "1001") should be (List(1,-2,-3, -4))
    SatMapReduceHelper.createMap(List(1, 2, 3), "111") should be (List(1,2,3))
    SatMapReduceHelper.createMap(List(1, 2, 3, -5, -4), "00101") should be (List(-1,-2,3, 5, -4))
  }

  "A list of ints" should "be translated to the corresponding string" in {
    SatMapReduceHelper.createSatString(List(1,2,-3,4,-10,8)) should be ("1 2 -3 4 -10 8")
    SatMapReduceHelper.createSatString(List(1,2,-3)) should be ("1 2 -3")
    SatMapReduceHelper.createSatString(List(-1,-2,3,-4)) should be ("-1 -2 3 -4")
  }

}

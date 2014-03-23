package main.scala.common

import main.scala.utils.{ISatCallback, ConvertionHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

/**
 * Created by marbarfa on 3/3/14.
 */
object SatMapReduceHelper extends ConvertionHelper {

  /**
   * This method returns some random (or not) variables to be used to split the problem
   * among the mappers.
   * eg: if input is (1,2) and there are 10 variables, the return could return List(3,4)
   * @param fixedVars
   * @param n
   * @return
   */
  def generateProblemSplit(fixedVars: List[Int], n: Int, amount: Int): List[Int] = {
    var variables: List[Int] = List[Int]();
    for (i <- 0 until n) {
      if (!fixedVars.contains(i)) {
        variables = i :: variables;
      }
      if (variables.size >= amount) {
        return variables;
      }
    }
    return variables;
  }


  /**
   * Given an input string with the instance specification, returns a map consisting of
   * (literal -> boolean_value).
   * @param instance
   * @return
   */
  def parseInstanceDef(instance: String) : Map[Int, Boolean] =
    instance
      .split(" ")
      .map(x => (math.abs(x.toInt), if (x.toInt > 0) true else false))
      .toMap


  /**
   * based on a list of variables and an int representation of the binary value (eg: 3 = 11)
   * returns a Map of (key=variable => value=true|false))
   * eg: List(1,2,3), intBinaryValue=5("101") ==> Map(1->true, 2->false, 3->true)
   * @param vars
   * @param intBinaryValue
   * @return
  */
  def createMap(vars : List[Int], intBinaryValue : Int) : Map[Int,
    Boolean] ={
    var res = Map[Int,Boolean]()
    for(i <- 0 until vars.size){
      var boolValue : Boolean = intBinaryValue & Math.pow(2, i).toInt
      res += (vars(i) -> boolValue)
    }
    return res;
  }

  def genearteProblemMap(possibleVars : List[Int], callback : ISatCallback[Map[Int,Boolean]]){
    var maxValue  = math.pow(2,possibleVars.size).toInt
    //try to assign values to the selected literals
    for(i <- 0 until maxValue){
      var subproblem = createMap(possibleVars, i)
      callback apply subproblem     
    }
  }



  /**
   * Saves in a file in the inputPath a line with a subproblem definition.
   * eg: literals = Map(1:true,2:false) => it will save a file in HDFS with the following lines:
   * 1 -2
   * @param literals variableVars variables
   * @param savePath where to save (file name) the subproblem definition.
   */
  def saveProblemSplit(literals: Map[Int, Boolean], savePath: String) {

    val fs = FileSystem.get(new Configuration())
    var out: FSDataOutputStream = fs.create(new Path(savePath));

    out.writeBytes(createSatString(literals));
    out.flush();
    out.close();
  }

  //eg: for Map(1 -> true, 2->false, 3->false) ===> definition = "1 -2 -3"
  def createSatString(literals : Map[Int, Boolean]) : String =
    literals
      .keySet
      .foldLeft("")((varStr, b) =>
      varStr + " " + (if (literals(b)) b else -b).toString + ":")

  /**
   * Converts a int value to a binary string of @digits digits.
   * @param i
   * @param digits
   * @return
   */
  def toBinary(i: Int, digits: Int = 8) =
    String.format("%" + digits + "s", i.toBinaryString).replace(' ', '0')


}

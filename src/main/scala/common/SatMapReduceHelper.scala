package main.scala.common

import java.io._
import main.scala.utils.{SatLoggingUtils, ISatCallback, ConvertionHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.immutable.HashSet


/**
 * Created by marbarfa on 3/3/14.
 */
object SatMapReduceHelper extends ConvertionHelper with SatLoggingUtils {

  /**
   * This method returns some random (or not) variables to be used to split the problem
   * among the mappers.
   * eg: if input is (1,2) and there are 10 variables, the return could return List(3,4)
   * @param fixedVars
   * @param n
   * @return
   */
  def generateProblemSplit(fixedVars: List[Int], n: Int, amount: Int): List[Int] = {
    log.info(s"Generating subproblem split. Fixed: ${fixedVars.toString()}, n: $n, amount: $amount")
    var variables: List[Int] = List[Int]();
    (1 to n)
      .toStream
      .takeWhile(_ => variables.size < amount)
      .foreach(i => {
      if (!fixedVars.contains(i) && !fixedVars.contains(-i)) {
        variables = i :: variables;
      }
    });

    return variables;
  }


  /**
   * Given an input string with the instance specification, returns a map consisting of
   * (literal -> boolean_value).
   * @param instance
   * @return
   */
  def parseInstanceDef(instance: String): Set[Int] = {
    log.info(s"Parsing instance def: $instance")
    instance.trim
      .split(" ")
      .map(x => Integer.parseInt(x))
      .toSet
  }


  /**
   * based on a list of variables and an int representation of the binary value (eg: 3 = 11)
   * returns a Map of (key=variable => value=true|false))
   * eg: List(1,2,3), intBinaryValue=5("101") ==> Map(1->true, 2->false, 3->true)
   * @param vars
   * @param intBinaryValue
   * @return
   */
  def createMap(vars: List[Int], intBinaryValue: String): Set[Int] = {
    var res = new HashSet[Int]()
    for (i <- 0 until vars.size) {
      if (Integer.parseInt(intBinaryValue.charAt(i).toString) == 0){
        res += -vars(i)
      }else {
        res += vars(i)
      }
    }
    return res;
  }

  def genearteProblemMap(possibleVars: List[Int], callback: ISatCallback[Set[Int]]) {
    var maxValue = math.pow(2, possibleVars.size).toInt
    //try to assign values to the selected literals
    for (i <- 0 until maxValue) {
      //i from 0 to 4 (eg: possibleVars.size = 2)
      var subproblem = createMap(possibleVars, toBinary(i, possibleVars.size))
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
  def saveProblemSplit(literals: Set[Int], savePath: String) {
    var saveStr = createSatString(literals)
    val fs = FileSystem.get(new Configuration())

    try{
      var breader=new BufferedReader(new InputStreamReader(fs.open(new Path(savePath))));
      var l = breader.readLine();
      while (l != null){
        saveStr = saveStr + "\n" + l;
        l = breader.readLine()
      }
      breader.close()
    } catch {
      case e : Throwable => log.error("Error reading file")
    }

    var br = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(savePath))));
    br.write(saveStr);
    br.flush()
    br.close()


  }

  //eg: for Map(1 -> true, 2->false, 3->false) ===> definition = "1 -2 -3"
  def createSatString(literals: Set[Int]): String =
    literals
      .foldLeft("")((varStr, b) =>
      varStr + " " + b)

  /**
   * Converts a int value to a binary string of @digits digits.
   * @param i
   * @param digits
   * @return
   */
  def toBinary(i: Int, digits: Int = 8) =
    String.format("%" + digits + "s", i.toBinaryString).replace(' ', '0')


}

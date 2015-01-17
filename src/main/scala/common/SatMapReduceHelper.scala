package main.scala.common

import java.io.{OutputStreamWriter, BufferedWriter, BufferedReader, InputStreamReader}
import main.scala.domain.Formula
import main.scala.utils.{ISatCallback, SatLoggingUtils, ConvertionHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
 * Created by marbarfa on 3/3/14.
 */
object SatMapReduceHelper extends ConvertionHelper with SatLoggingUtils {

  /**
   * This method returns some random (or not) variables to be used to split the problem
   * among the mappers.
   * eg: if input is (1,2) and there are 10 variables, the return could return List(3,4)
   * @return
   */
  def generateProblemSplit(fixedVars: List[Int], amount: Int, formula: Formula): List[Int] = {
    log.debug(s"Generating subproblem split. Fixed: ${fixedVars.toString()}, n: ${formula.n}, amount: $amount")
    var variables: List[Int] = List[Int]();
    for(i <- formula.getLiteralsInOrder() if variables.size < amount){
      if (!fixedVars.contains(i) && !fixedVars.contains(-i)) {
        variables = i :: variables;
      }
    };

    return variables;
  }


  /**
   * Given an input string with the instance specification, returns a map consisting of
   * (literal -> boolean_value).
   * @param instance
   * @return
   */
  def parseInstanceDef(instance: String): List[Int] = {
    log.debug(s"Parsing instance def: $instance")
    instance.trim
      .split(" ")
      .map(x => if (x.length > 0)
                  Integer.parseInt(x)
                else
                  0)
      .filter(x => x>0)
      .toList

  }


  /**
   * based on a list of variables and an int representation of the binary value (eg: 3 = 11)
   * returns a Map of (key=variable => value=true|false))
   * eg: List(1,2,3), intBinaryValue=5("101") ==> Map(1->true, 2->false, 3->true)
   * @param vars
   * @param intBinaryValue
   * @return
   */
  def createMap(vars: List[Int], intBinaryValue: String): List[Int] = {
    var res = List[Int]()
    for (i <- 0 until vars.size) {
      if (Integer.parseInt(intBinaryValue.charAt(i).toString) == 0) {
        res = res ++ List(-vars(i))
      } else {
        res = res ++ List(vars(i))
      }
    }
    return res;
  }

  def genearteProblemMap(possibleVars: List[Int], callback: ISatCallback[List[Int]]) {
    var maxValue = math.pow(2, possibleVars.size).toInt
    //try to assign values to the selected literals
    for (i <- 0 until maxValue) {
      try {
        //i from 0 to 4 (eg: possibleVars.size = 2)
        var subproblem = createMap(possibleVars, toBinary(i, possibleVars.size))
        callback apply subproblem
      } catch {
        case t: Throwable => log.error(s"Error creating problem map for $i, possibleVars: ${possibleVars.toString()}", t)
      }
    }
  }


  /**
   * Saves in a file in the inputPath a line with a subproblem definition.
   * eg: literals = Map(1:true,2:false) => it will save a file in HDFS with the following lines:
   * 1 -2
   * @param literals variableVars variables
   * @param savePath where to save (file name) the subproblem definition.
   */
  def saveProblemSplit(literals: List[Int], savePath: String) {
    var saveStr = createSatString(literals)
    saveStringToFile(saveStr, savePath, true);
  }


  /**
   * Saves a string @str to a file in @savePath.
   * If @append is true => appends the @str string to the file if it exists.
   * @param savePath where to save (file name) the subproblem definition.
   */
  def saveStringToFile(str: String, savePath: String, append: Boolean) {
    val fs = FileSystem.get(new Configuration())
    var saveStr = str
    if (append) {
      try {
        var breader = new BufferedReader(new InputStreamReader(fs.open(new Path(savePath))));
        var l = breader.readLine();
        while (l != null) {
          saveStr = saveStr + "\n" + l;
          l = breader.readLine()
        }
        breader.close()
      } catch {
        case e: Throwable => //do nothing.
      }
    }

    var br = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(savePath))));
    br.write(saveStr);
    br.flush()
    br.close()
  }

  //eg: for Map(1 -> true, 2->false, 3->false) ===> definition = "1 -2 -3"
  def createSatString(literals: List[Int]): String =
    literals
      .sorted
      .foldLeft("")((varStr, b) =>
      varStr + " " + b) + "\n"

  /**
   * Converts a int value to a binary string of @digits digits.
   * @param i
   * @param digits
   * @return
   */
  def toBinary(i: Int, digits: Int = 8) =
    String.format("%" + digits + "s", i.toBinaryString).replace(' ', '0')


}

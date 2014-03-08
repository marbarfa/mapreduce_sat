package scala.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

/**
 * Created by marbarfa on 3/3/14.
 */
object SatMapReduceHelper {

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
   * Saves in a file in the inputPath a line with a subproblem definition.
   * each line will be, for example:
   * let variableVars = (1,2), fixedVars= (3 => true) => it will save a file in HDFS with the following lines:
   * 3;-1 -2
   * 3;-1 2
   * 3;1 -2
   * 3;1 2
   * @param variableVars variableVars variables
   * @param savePath where to save (file name) the subproblem definition.
   */
  def saveProblemSplit(fixedVars : List[Int], variableVars: List[Int], savePath: String) {
    val linesCant = Math.pow(variableVars.size, 2).toInt
    var strValues: Array[String] = new Array[String](linesCant)
    var varVals = 0

    //eg: for List(1,2,3,4) => fixedStr = "1 2 3 4"
    var fixedStr : String = fixedVars.foldLeft("") ((a,b) => a + " " + b.toString)


    for (i <- 0 until linesCant) {
      var str = fixedStr + ";" + generateSubproblemLine(variableVars, toBinary(varVals, variableVars.size))
      strValues(i) = str;
      varVals += 1;
    }

    val fs = FileSystem.get(new Configuration())
    val out: FSDataOutputStream = fs.create(new Path(savePath))
    for(line <- strValues) out.writeBytes(line + "\n")
    out.flush();
    out.close();
  }

  /**
   * Converts a int value to a binary string of @digits digits.
   * @param i
   * @param digits
   * @return
   */
  def toBinary(i: Int, digits: Int = 8) =
    String.format("%" + digits + "s", i.toBinaryString).replace(' ', '0')

  /**
   * Guiven a list of variableVars variables (eg: (1,2,3)) and a value string (eg: 010) generate a subproblem
   * string (eg:-1 2 -3). This value string will be used as a 3sat subproblem statement to be used by mappers.
   * @param fixed
   * @param valueString
   * @return
   */
  private def generateSubproblemLine(fixed : List[Int], valueString : String) : String = {
      var res : String = ""
    for(i <- 0 until fixed.size){
      res += Math.abs(fixed(i)) * Integer.parseInt(valueString.charAt(fixed.size-1 -i).toString)
    }
    res = res.trim
    return res;
  }

  /**
   * given varVal as a decimal value of the possible value that can take the variableVars variables and n as the number of
   * varibables, this method returns the string value to be set in a subproblem specification.
   * eg: varVal = 1, n = 2 ==> 01
   * varVal = 3, n=4 ===> 0011
   * @param varVal
   * @param n
   * @return
   */
  private def getVarValueString(varVal: Int, n: Int): String = {
    var res = ""
    var binaryValue = varVal.toBinaryString;
    res = binaryValue;
    if (binaryValue.length < n) {
      //add zeros in the left
      for (i <- binaryValue.length to n) {
        res = "0" + binaryValue;
      }
    }

    return res;
  }

}

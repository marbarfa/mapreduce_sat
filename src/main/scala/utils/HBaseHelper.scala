package utils

import java.security.MessageDigest

import main.scala.domain.Formula
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HTable}
import sun.security.provider.MD5

/**
 * Created by mbarreto on 1/17/15.
 */
trait HBaseHelper extends SatLoggingUtils {

  var table: HTable = _

  def initHTable() {
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  protected def retrieveInvalidLiterals: List[List[Int]] = {
    var invalidLiterals = List[List[Int]]()
    var scanner = table.getScanner("invalid_literals".getBytes, "a".getBytes);
    try {
      val it = scanner.iterator();

      while (it.hasNext){
        val rr = it.next();
        val rowStr = new String(rr.getRow);

        val splitrow = rowStr.split(" ")
        var setOfLiterals = List[Int]()
        splitrow.foreach(s => {
          try {
            setOfLiterals = setOfLiterals ++ List(s.toInt)
          } catch {
            case e: Throwable => //parsed empty of "_" character.
          }
        })
        invalidLiterals = invalidLiterals ++ List(setOfLiterals)
      }
    } finally {
      scanner.close();
    }

    return invalidLiterals;
  }

  def retrieveSolution(problem: String) : Boolean = {
    val problemSplit = problem.split("/").last;
    var scanner = table.getScanner("solution".getBytes, problemSplit.getBytes);
    try{
      val it = scanner.iterator()
      if (it != null && it.hasNext){
        return true;
      }
    } catch {
      case e : Throwable => log.info(" Error retrieving error");
    }
    return false;
  }

  protected def stringToIntSet(str : String) : List[Int] =
    str.split(" ").foldLeft(List[Int]())((acc, b) =>
      try {
        var intVal = b.trim.toInt
        acc ++ List(intVal);
      } catch {
        case e: Throwable => //parsed empty of "_" character.
          acc
      }
    );

  private def getLiteralsPathFromMap(key: String, hbaseInfo: Map[String, List[String]]) : List[List[Int]] ={
    var res = List(stringToIntSet(key))
    if (hbaseInfo contains key){
      hbaseInfo.getOrElse(key.trim, List()).foreach(s => {
        var partialRes = List[Int]()
        var paths =  getLiteralsPathFromMap(s, hbaseInfo);
        paths.foreach(l => partialRes = partialRes ++ l)
        res = List(partialRes) ++ res
        log.info(s"Returning literal path $res")
      })
    }
    return res;
  }

  /**
   * Retrieves from HBase all possible paths for the @key passed.
   * Example: Having in Hbase
   * 8 -7 ---> 6 -5|6 5
   * 6 -5 ---> 4 3|4 -3
   * 6 5  ---> 4 -3
   * 4 3 ----> 1 -2
   * 4 -3 ---> -1 -2
   *
   * Let @key be '8 -7' then the result would be:
   * [[8 - 7 6 - 5 4 3 1 - 2 ], [ 8 - 7 6 5 4 - 3 - 1 - 2]]
   *
   * @param key
   * @return
   */
  protected def retrieveLiteralsPaths(key: String): List[List[Int]] = {
    var litPaths = List[List[Int]]()
    var scanner = table.getScanner("path".getBytes, "a".getBytes);
    var hbaseInfo = Map[String, List[String]]()
    try {

      var it = scanner.iterator();
      while (it.hasNext) {
        var rr = it.next();
        var rowStr = new String(rr.getRow);
        var valStr = new String(rr.value);
        log.info(s"[LitPath]Found row $rowStr")
        log.info(s"[LitPath]Found val $valStr")
        log.info(s"List: ${valStr.split("&").toList}")
        hbaseInfo = Map(rowStr.trim -> valStr.split("&").toList) ++ hbaseInfo
      }
      log.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      log.info(s"Created map $hbaseInfo")
      log.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      return getLiteralsPathFromMap(key, hbaseInfo)
    } finally {
      scanner.close();
    }

    return litPaths;
  }

  def saveToHBaseInvalidLiteral(key: String, value: String) {
    var put = new Put(key.getBytes)
    put.add("invalid_literals".getBytes, "a".getBytes, value.getBytes);
    table.put(put);
    log.trace(s"Key [${key}] saved...")
  }

  def saveToHBaseSolFound(sol: String, problem: String, time : Long) {
    val problemSplit = problem.split("/").last;
    var put = new Put(s"solution_${problemSplit}_${((System.currentTimeMillis() - time) / 1000)}s".getBytes)
    put.add("solution".getBytes, problemSplit.getBytes, sol.getBytes);
    table.put(put);
    log.trace(s"Key [$sol] with value [$sol] saved...")
  }

  protected def md5(s : String): String = MessageDigest.getInstance("MD5").digest(s.getBytes).toString

  def saveToHBaseFormula(assignment: String, formula: Formula) = {
    var md5Key = md5(assignment)
    var put = new Put(md5Key.getBytes)
    put.add("formulas".getBytes, "a".getBytes, formula.toCNF.getBytes);
    table.put(put);
  }

  def saveToHBaseLiteralPath(fixedLiterals: String, foundLiterals: String) {
    var put = new Put(fixedLiterals.getBytes)
    put.add("path".getBytes, "a".getBytes, foundLiterals.getBytes);
    table.put(put);
    log.trace(s"Key [$fixedLiterals] with value [$foundLiterals] saved...")
  }


}

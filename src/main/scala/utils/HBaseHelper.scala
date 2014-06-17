package main.scala.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HTable}
import scala.collection.JavaConversions._

/**
 * Created by marbarfa on 6/3/14.
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
      scanner.iterator().toStream.foreach(rr => {
        var rowStr = new String(rr.getRow);

        var splitrow = rowStr.split(" ")
        var setOfLiterals = List[Int]()
        splitrow.foreach(s => {
          try {
            setOfLiterals = setOfLiterals ++ List(s.toInt)
          } catch {
            case e: Throwable => //parsed empty of "_" character.
          }
        })
        invalidLiterals = invalidLiterals ++ List(setOfLiterals)
      });
    } finally {
      scanner.close();
    }

    return invalidLiterals;
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
    var res = List[List[Int]]()
    if (hbaseInfo contains key){
      hbaseInfo.getOrElse(key.trim, List()).foreach(s => {
        var partialRes = List[Int]()
        var paths =  getLiteralsPathFromMap(s, hbaseInfo);
        paths.foreach(l => partialRes = partialRes ++ l)
        partialRes = partialRes ++ stringToIntSet(key)
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
      scanner.iterator().toStream.foreach(rr => {
        var rowStr = new String(rr.getRow);
        var valStr = new String(rr.value);
        log.info(s"[LitPath]Found row $rowStr")
        log.info(s"[LitPath]Found val $valStr")
        log.info(s"List: ${valStr.split("&").toList}")
        hbaseInfo = Map(rowStr.trim -> valStr.split("&").toList) ++ hbaseInfo
      })
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

  def saveToHBaseLiteralPath(fixedLiterals: String, foundLiterals: String) {
    var put = new Put(fixedLiterals.getBytes)
    put.add("path".getBytes, "a".getBytes, foundLiterals.getBytes);
    table.put(put);
    log.trace(s"Key [$fixedLiterals] with value [$foundLiterals] saved...")
  }


}

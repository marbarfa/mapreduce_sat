package scala.hadoop

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Get, HTable}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.domain.Formula
import scala.utils.CacheHelper

/**
 *
 * HBase table "var_tables" has only one column: LITERAL_COMBINATION | CLAUSE
 * If variables (x1 = 0, x5=1, x3=0  make clause 3 false then there will be a registry in HBase like:
 * "1:0 3:0 5:1" --> "3"
 *
 *
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[LongWritable, Text, Text, Text] {
  var formula: Formula = _

  var table: HTable = _

  protected override def setup(context: Mapper#Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance
    }

    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  /**
   * The mapper key will be
   * @param key line offset
   * @param value subproblem definition like: (1 -2;3 4) --> fixed values: 1=true and 2=false and
   *              2 variables: (3=false,4=false)
   * @param context
   */
  override def map(key: LongWritable, value: Text, context: Mapper#Context) {
    var d = CacheHelper.depth
    var fixedLiterals = value.toString.split(" ").toList.map(x => x.toInt)
    var possibleVars : List[Int] = getPossibleVar(fixedLiterals, d)

    var maxValue  = Math.pow(possibleVars.size, 2).toInt
    //try to assign values to the selected literals
    possibleVars.foreach(p => {
      p.
    })


  }


  private def getPossibleVar(vars: List[Int], d: Int): List[Int] = {
    var res = List[Int]()
    var v = getVar(vars);
    while (v.isDefined && res.size < d) {
      res = v.get :: res;
      v = getVar(vars ::: res);
    }

    return res;
  }

  private def getVar(vars: List[Int]): Option[Int] = {
    for (i <- 0 to formula.n) vars.foreach(v => if (v != i) return Option(i))
    return None;
  }

  private def existsInKnowledgeBase(vars: Map[Int, Boolean]): Boolean = {
    var varkey: String = ""
    vars
      .keySet
      .toList
      .sorted
      .foreach(k =>
      varkey += k.toString + bool2int(vars.getOrElse(k, false)))
    try {
      val result: Result = table.get(new Get(varkey.getBytes))
      if (result != null) {
        return true;
      }
    } catch {
      case t: Throwable => // variable combination not found.
    }
    return false;

  }

  def bool2int(b: Boolean) = if (b) 1 else 0


}

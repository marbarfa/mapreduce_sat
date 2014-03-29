package main.scala.hadoop

import main.scala.common.SatMapReduceHelper
import main.scala.domain.{Formula, Clause}
import main.scala.utils.{SatLoggingUtils, ConvertionHelper, CacheHelper, ISatCallback}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, HTable, Get}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
 *
 * HBase table "var_tables" has only one column: LITERAL_COMBINATION | CLAUSE
 * If variables (x1 = 0, x5=1, x3=0  make clause 3 false then there will be a registry in HBase like:
 * "1:0 3:0 5:1" --> "3"
 *
 *
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[LongWritable, Text, Text, Text] with ConvertionHelper with SatLoggingUtils {
  var formula: Formula = _
  var table: HTable = _
  type Context = Mapper[LongWritable, Text, Text, Text]#Context


  protected override def setup(context: Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance(context.getConfiguration.get("problem_path"))
    }

    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  /**
   * This method adds the combination of literals in clause to the DB (because make that clause false).
   * @param clause
   * @param literals
   */
  def addLiteralsToDB(clause: Clause, literals: Map[Int, Boolean]) {
    var key = literalMapToDBKey(literals)
    log.info(s"False combination of literals: ${key}")

    var put = new Put(key.toString.getBytes)
    put.add("cf".getBytes, "a".getBytes, clause.toString.getBytes);
  }


  /**
   * The mapper key will be
   * @param key line offset
   * @param value subproblem definition like: (1 -2 3 4) --> fixed values: 1=true and 2=false and
   *              2 variables: (3=false,4=false)
   * @param context
   */
  override def map(key: LongWritable, value: Text, context: Context) {
    var d = CacheHelper.depth
    log.info(s"Starting mapper with key $key, value: ${value.toString}, depth: $d")
    var fixedLiterals: Map[Int, Boolean] =  SatMapReduceHelper.parseInstanceDef(value.toString)

    var possibleVars: List[Int] = SatMapReduceHelper.generateProblemSplit(fixedLiterals.keySet.toList, formula.n, d)


    SatMapReduceHelper.genearteProblemMap(possibleVars, new ISatCallback[Map[Int, Boolean]] {
      override def apply(subproblem: Map[Int, Boolean]) = {
        log.info(s"Checking subproblem: ${literalMapToDBKey(subproblem)}")

        if (!existsInKnowledgeBase(subproblem)) {
          var satisfasiable = formula.isSatisfasiable(subproblem)
          if (!satisfasiable) {
            log.info ("Subproblem not a valid subsolution")
            //add variable combination to knowledge base.
            var clauses = formula.getFalseClauses(subproblem)
            clauses.foreach(clause => addLiteralsToDB(clause, subproblem));
          } else {
            //output key="fixed", value="subproblem"
            var satString = SatMapReduceHelper.createSatString(fixedLiterals ++ subproblem)
            log.info (s"Subproblem is a valid subsolution, output: $satString")
            context.write(value, new Text(satString.getBytes));
          }
        }
      }
    })
  }

  private def existsInKnowledgeBase(vars: Map[Int, Boolean]): Boolean = {
    var varkey: String = literalMapToDBKey(vars)
    try {
      val result: Result = table.get(new Get(varkey.getBytes))
      if (result != null) {
        return true;
      }
    } catch {
      case t: Throwable => log.error (s"Key not found in DB, error: ${t.getCause}")// variable combination not found.
    }
    return false;

  }


}

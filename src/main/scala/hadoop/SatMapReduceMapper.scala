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
  var numberOfSplits : Int = _
  var table: HTable = _
  type Context = Mapper[LongWritable, Text, Text, Text]#Context


  protected override def setup(context: Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance(context.getConfiguration.get("problem_path"))
    }
    numberOfSplits = context.getConfiguration.get("numbers_of_mappers").toInt


    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  protected override def cleanup(context: Context){
    formula = null;
    table = null;
  }



  /**
   * This method adds the combination of literals in clause to the DB (because make that clause false).
   * @param clause
   * @param literals
   */
  def addLiteralsToDB(clause: Clause, literals: Set[Int]) {
    var key = literalMapToDBKey(literals)
    log.debug(s"False combination of literals: ${key}")

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
    var start = System.currentTimeMillis();
    log.info(s"Starting mapper with key $key, value: ${value.toString}, depth: $numberOfSplits")
    var fixedLiterals: Set[Int] =  SatMapReduceHelper.parseInstanceDef(value.toString)

    var possibleVars: List[Int] = SatMapReduceHelper.generateProblemSplit(fixedLiterals.toList, formula.n, numberOfSplits)
    log.debug(s"Mapper possible vars: ${possibleVars.toString()}")
    var validSubsolutions : Int = 0
    var invalidSubsolutions : Int = 0

    SatMapReduceHelper.genearteProblemMap(possibleVars, new ISatCallback[Set[Int]] {
      override def apply(subproblem: Set[Int]) = {
        var problemDef = fixedLiterals ++ subproblem
        log.debug(s"Checking subproblem: ${subproblem.toString()} | " +
          s"fixed: ${fixedLiterals.toString()} | " +
          s"problemDef: ${problemDef.toString}")

        if (!existsInKnowledgeBase(problemDef)) {
          var satisfasiable = formula.isSatisfasiable(problemDef)
          if (!satisfasiable) {
            invalidSubsolutions = invalidSubsolutions+1;
            log.debug ("Subproblem not a valid subsolution")
            //add variable combination to knowledge base.
            var clauses = formula.getFalseClauses(problemDef)
            clauses.foreach(clause => addLiteralsToDB(clause, problemDef));
          } else {
            validSubsolutions = validSubsolutions+1;
            //output key="fixed", value="subproblem"
            var satString = SatMapReduceHelper.createSatString(subproblem)
            log.debug (s"Subproblem is a valid subsolution, output: $satString")
            context.write(value, new Text(satString.getBytes));
          }
        }
      }
    })

    log.info(
      s"""
         |Finishing... ### Mapper Stats ###:
         |Subsolutions = valid: $validSubsolutions | invalid: $invalidSubsolutions
         |ExecTime: ${(System.currentTimeMillis() - start)/1000} seconds
       """.stripMargin);
  }

  private def existsInKnowledgeBase(vars: Set[Int]): Boolean = {
    var varkey: String = literalMapToDBKey(vars)
    log.debug(s"Searching for key $varkey")
    try {
      val result: Result = table.get(new Get(varkey.getBytes))
      if (result != null && result.getRow !=null) {
        log.info(s"Key found in db: ${result.toString} | row: ${result.getRow} | exists? ${result.getExists.toString}")
        return true;
      }
    } catch {
      case t: Throwable => log.error (s"Key not found in DB, error: ${t.getCause}")// variable combination not found.
    }
    return false;

  }


}

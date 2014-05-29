package main.scala.hadoop

import main.scala.common.SatMapReduceHelper
import main.scala.domain.{Formula, Clause}
import main.scala.utils.{SatLoggingUtils, ConvertionHelper, CacheHelper}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.collection.JavaConversions._

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
  var numberOfSplits: Int = _
  var table: HTable = _
  type Context = Mapper[LongWritable, Text, Text, Text]#Context

  var invalidLiterals : Set[Set[Int]] = Set[Set[Int]]()


  protected override def setup(context: Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance(context.getConfiguration.get("problem_path"))
    }
    numberOfSplits = context.getConfiguration.get("numbers_of_mappers").toInt

    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")


    var scanner = table.getScanner("cf".getBytes, "a".getBytes);
    try {
      scanner.iterator().toStream.foreach(rr => {
        var rowStr = new String(rr.getRow);
        log.info(s"Found row $rowStr")

        var splitrow = rowStr.split(" ")
        var setOfLiterals = Set[Int]()
        splitrow.foreach(s => {
          try{
            setOfLiterals = setOfLiterals + s.toInt
          } catch {
            case e : Throwable => //parsed empty of "_" character.
          }
        })
        log.info(s"Set of literals retrieved from HBase: ${setOfLiterals.toString()}.")
        invalidLiterals = invalidLiterals + setOfLiterals
      });

    } finally {
      scanner.close();
    }

  }

  protected override def cleanup(context: Context) {
    formula = null;
    table = null;
  }


  /**
   * This method adds the combination of literals in clause to the DB (because make that clause false).
   * @param clause
   * @param literals
   */
  def addLiteralsToDB(clause: Clause, literals: Set[Int]) {
    var key : String = ""
    key = clause.literals.foldLeft("")((acc, x) => {
      var l = literals.find(y => y==x || y == -x).getOrElse(0)
      if (l != 0) {
          acc + " " + l
      }else{
        acc
      }
    }).trim;

    log.info(s"False combination of literals: [$key] for clause ${clause.id}")

    var put = new Put(key.getBytes)
    put.add("cf".getBytes, "a".getBytes, clause.toString.getBytes);
    table.put(put);
    log.info (s"Key [${key}] saved...")

  }


  /**
   * The mapper key will be
   * @param key line offset
   * @param value subproblem definition like: (1 -2 3 4) --> fixed values: 1=true and 2=false and
   *              2 variables: (3=false,4=false)
   * @param context
   */
  override def map(key: LongWritable, value: Text, context: Context) {
    var start = System.currentTimeMillis();
    log.info(s"Starting mapper with key $key, value: ${value.toString}, depth: $numberOfSplits")
    var fixed: Set[Int] = SatMapReduceHelper.parseInstanceDef(value.toString)

    var execStats = searchForLiterals(fixed, Set(), value, context, numberOfSplits);

    log.info(
      s"""
         |Finishing... ### Mapper Stats ###
         |ExecTime: ${(System.currentTimeMillis() - start) / 1000} seconds
         |Sols found : ${execStats._1} | Pruned: ${execStats._2}
       """.stripMargin);
  }

  def searchForLiterals(fixed: Set[Int], selected: Set[Int], value: Text, context: Context, depth: Int): (Int, Int) = {
    if (depth == 0) {
      var satString = SatMapReduceHelper.createSatString(fixed ++ selected)
      log.debug(s"Subproblem is a valid subsolution, output: $satString")
      context.write(value, new Text(satString.getBytes));
      return (1, 0)
    } else {
      var subsolsFound = 0
      var pruned = 0;
      var fixedSubproblem = fixed ++ selected
      var l = selectLiteral(fixedSubproblem);
      if (l != 0) {
        //recursive part..
        val subproblem = fixedSubproblem ++ Set(l)
        if (!evaluateSubproblem(subproblem)) {
          var clauses = formula.getFalseClauses(subproblem)
          clauses.foreach(clause => addLiteralsToDB(clause, subproblem));
          //prune => do not search in this branch.
          pruned = pruned + 1;
        } else {
          var ij = searchForLiterals(fixed, selected ++ Set(l), value, context, depth - 1);
          subsolsFound = subsolsFound + ij._1
          pruned = pruned + ij._2
        }

        val subproblemPositive = fixedSubproblem ++ Set(-l)
        if (!evaluateSubproblem(subproblemPositive)) {
          var clauses = formula.getFalseClauses(subproblemPositive)
          clauses.foreach(clause => addLiteralsToDB(clause, subproblemPositive));
          //prune => do not search in this branch.
          pruned = pruned + 1;
        } else {
          var ij = searchForLiterals(fixed, selected ++ Set(-l), value, context, depth - 1);
          subsolsFound = subsolsFound + ij._1
          pruned = pruned + ij._2
        }
      }
      return (subsolsFound, pruned);
    }
  }

  private def evaluateSubproblem(subproblem: Set[Int]): Boolean = {
    if (!existsInKnowledgeBase(subproblem)) {
      return formula.isSatisfasiable(subproblem)
    }
    return false;
  }

  private def selectLiteral(vars: Set[Int]): Int = {
    (1 to formula.n).foreach(x => {
      if (!vars.contains(x) && !vars.contains(-x)) {
        return x;
      }
    })
    return 0;
  }


  private def existsInKnowledgeBase(vars: Set[Int]): Boolean = {
    var found = false;
    for(invalidSet <- invalidLiterals if !found){
      if (invalidSet subsetOf vars){
        found = true
      }
    }
    return false;

  }


}

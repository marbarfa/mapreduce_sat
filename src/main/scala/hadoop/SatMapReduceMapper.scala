package main.scala.hadoop

import java.util.Date

import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.domain.{Formula, Clause}
import main.scala.utils.{HBaseHelper, SatLoggingUtils, ConvertionHelper, CacheHelper}
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
class SatMapReduceMapper extends Mapper[LongWritable, Text, LongWritable, Text] with ConvertionHelper
with SatLoggingUtils with HBaseHelper {

  var formula: Formula = _
  var depth: Int = _
  var iteration : Int = _
  var withHbase = true
  var satProblem : String = _
  var solFound : Boolean = _
  var startTime : Long = _
  type Context = Mapper[LongWritable, Text, LongWritable, Text]#Context

  var invalidLiterals: List[List[Int]] = List[List[Int]]()

  protected override def setup(context: Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance(context.getConfiguration.get(SatMapReduceConstants.config.problem_path))
    }

    satProblem = context.getConfiguration.get("problem_path");
    depth = context.getConfiguration.get(SatMapReduceConstants.config.depth).toInt
    iteration = context.getConfiguration.get(SatMapReduceConstants.config.iteration).toInt
    startTime = context.getConfiguration.getLong("start_miliseconds", 0);


    if (withHbase){
      initHTable();
      solFound = retrieveSolution(satProblem)
      if (!solFound){
        invalidLiterals = retrieveInvalidLiterals
        log.debug("------------------------------------")
        log.debug(s"Invalid literals combinations:")
        invalidLiterals.foreach(l => {
          log.debug(s"[${l.toString()}}]")
        })
        log.trace("------------------------------------")
      }
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
  def addLiteralsToDB(clause: Clause, literals: List[Int]) {
    log.debug(s"Adding literals $literals to clause ${clause.id}=${clause.literals}")
    var key: String = ""
    key = clause.literals.foldLeft("")((acc, x) => {
      acc + " " + (-x)
    }).trim;
    log.debug(s"Adding key: $key with value ${clause.literals.toString()}")
    saveToHBaseInvalidLiteral(key, clause.literals.toString())
  }


  /**
   * The mapper key will be
   * @param key line offset
   * @param value subproblem definition like: (1 -2 3 4) --> fixed values: 1=true and 2=false and
   *              2 variables: (3=false,4=false)
   * @param context
   */
  override def map(key: LongWritable, value: Text, context: Context) {
    val start = System.currentTimeMillis();
    if (!solFound){
      val fixed: List[Int] = SatMapReduceHelper.parseInstanceDef(value.toString)
      log.debug(s"[Iteration $iteration|fixed: ${fixed.size} Mapper value: ${value.toString}, fixed: ${fixed.toString()}")
      if (fixed.size > 0) {
        val execStats = searchForLiterals(fixed, List(), key, context, depth);

        log.info(
          s"""
         |##################    Mapper Stats   ###################
         |Mapper thread ID: ${Thread.currentThread().getId}
         ||Mapper thread Name: ${Thread.currentThread().getName}
         |Iteration $iteration
         |fixed: ${fixed.size}
         |ExecTime: ${(System.currentTimeMillis() - start) / 1000} seconds
         |Sols found : ${execStats._1} | Pruned: ${execStats._2}
         |########################################################
       """.stripMargin);
      }
    }

  }

  /**
   * @param fixed list of fixed variables
   * @param selected list of new seleted literals to fix.
   * @param context current context.
   * @param depth depth to search.
   * @return (x,y) where x is the number of possible solutions found and the y is the number of prunned branches.
   */
  def searchForLiterals(fixed: List[Int], selected: List[Int], key: LongWritable, context: Context, depth: Int): (Int, Int) = {
    if (depth == 0) {
      val satSubproblem = SatMapReduceHelper.createSatString(fixed ++ selected)
      context.write(new LongWritable(Thread.currentThread().getId), new Text(satSubproblem.getBytes));
      return (1, 0)
    } else {
      var subsolsFound = 0
      var pruned = 0;
      val fixedSubproblem = fixed ++ selected
      //select one literal to fix.
      val l = selectLiteral(fixedSubproblem);
      if (l != 0) {
        //recursive part..
        val subproblem = fixedSubproblem ++ Set(l)
        if (!evaluateSubproblem(subproblem)) {
          var clauses = formula.getFalseClauses(subproblem)
          if (withHbase){
            clauses.foreach(clause => addLiteralsToDB(clause, subproblem));
          }
          //prune => do not search in this branch.
          pruned = pruned + 1;
        } else {
          var ij = searchForLiterals(fixed, selected ++ Set(l), key, context, depth - 1);
          subsolsFound = subsolsFound + ij._1
          pruned = pruned + ij._2
        }
        val subproblemPositive = fixedSubproblem ++ Set(-l)
        log.debug(s"Selected -$l, subproblem: $subproblemPositive")
        if (!evaluateSubproblem(subproblemPositive)) {
          val clauses = formula.getFalseClauses(subproblemPositive)
          log.debug(s"Problem instance $subproblemPositive makes the following clauses false:")
          clauses.foreach(c => log.debug(s"clause: ${c.literals} ${c.id}"))

          if (withHbase) {
            clauses.foreach(clause => addLiteralsToDB(clause, subproblemPositive));
          }
          //prune => do not search in this branch.
          pruned = pruned + 1
        } else {
          var ij = searchForLiterals(fixed, selected ++ Set(-l), key, context, depth - 1);
          subsolsFound = subsolsFound + ij._1
          pruned = pruned + ij._2
        }
      }else{
        if ((fixed ++ selected).size == formula.n){
          //all literals set!, check if its a solution:
          val satSelectedLiterals = SatMapReduceHelper.createSatString(fixed ++ selected)
          if (formula.isSatisfasiable(fixed ++ selected, log)){
            val satSelectedLiterals = SatMapReduceHelper.createSatString(fixed ++ selected)
            context.write(new LongWritable(Thread.currentThread().getId), new Text(satSelectedLiterals));
            saveToHBaseSolFound(satSelectedLiterals, satProblem, startTime)
          }else {
            context.write(new LongWritable(Thread.currentThread().getId), new Text(satSelectedLiterals));
          }
        }
        subsolsFound = 1;
        pruned = 0;
      }
      return (subsolsFound, pruned);
    }
  }

  private def evaluateSubproblem(subproblem: List[Int]): Boolean = {
//    if (!withHbase || !existsInKnowledgeBase(subproblem)) {
      return formula.isSatisfasiable(subproblem, log)
//    }
//    return false;
  }

  private def selectLiteral(vars: List[Int]): Int = {
    //iterate the literals by order of appearence in clauses.
    formula
      .getLiteralsInOrder()
      .slice(vars.size-1, formula.n)
      .foreach(x => {
      if (!vars.contains(x) && !vars.contains(-x)) {
        return x;
      }
    })
    return 0;
  }


  private def existsInKnowledgeBase(vars: List[Int]): Boolean = {
    var found = false;
    for (invalidSet <- invalidLiterals if !found) {
      if (invalidSet.toSet subsetOf vars.toSet) {
        log.debug(s"Set ${invalidSet.toSet} is a subset of ${vars.toSet}")
        found = true
      }
    }
    return found;

  }


}

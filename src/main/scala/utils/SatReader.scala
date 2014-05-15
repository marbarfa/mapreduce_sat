package main.scala.utils

import main.scala.common.SatMapReduceConstants
import main.scala.domain.{Formula, Clause}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.io.Source

/**
 * Created by marbarfa on 3/2/14.
 */
object SatReader extends ISatReader with SatLoggingUtils {


  /**
   * 3SAT instance reader.
   * @param instance_path instance problem path
   * @return 3SAT problem Formula.
   */
  override def read3SatInstance(instance_path: String): Formula = {
    var formula = new Formula();
    var clauses: Int = 0;
    var numberOfVars = 0;

    // read problem instance from file.
    try {
      val fs = FileSystem.get(new Configuration())

      for (line <- Source.fromInputStream(fs.open(new Path(instance_path))).getLines()) {
        //ignore commented lines => starting with # or with the character 'c'
        if (!line.startsWith("#") && !line.startsWith("c") && !line.isEmpty && !line.equals("%")) {
          //        log.info(s"Processing line $line")
          if (line.startsWith("p")) {
            //it has a problem definition => initialize.
            var problemDef: Array[String] = line.trim().split(" ")
            clauses = problemDef.apply(problemDef.size - 1).trim().toInt
            numberOfVars = problemDef.apply(problemDef.size - 2).trim().toInt
          } else {
            var clause = new Clause
            formula.clauses = clause :: formula.clauses
            //read each var of the current clause.
            line.trim().split(" ").foreach(v => {
              try {
                val readVar = Integer.parseInt(v)
                if (readVar != 0) {
                  // ignore 0 literals => should be the last one
                  clause.literals ::= readVar
                  formula.addClauseOfVar(readVar, clause);
                }
              } catch {
                case t: Throwable => {
                  log.error("Error parsing problem instance.", t);
                  throw t;
                }
              }
            })
          }
        }
      }

    } catch {
      case e: Throwable => {
        log.error(s"Error reading sat problem for file: ${instance_path}", e)
        throw e;
      }
    }
    formula.n = numberOfVars;
    formula.m = clauses;
    log.info(s"Problem instance read successfully: n=${formula.n}, m=${formula.m}")


    return formula;
  }

  /**
   * This method returns true if a solution is found.
   * @return
   */
  def readSolution(satProblem: String): Boolean = {
    log.info("Trying to read a 3SAT solution")
    var res = false;
    val fs = FileSystem.get(new Configuration())

    try {
      res = Source.fromInputStream(fs.open(new Path(SatMapReduceConstants.sat_solution_path + satProblem)))
        .getLines().size > 0
    } catch {
      case e: Throwable => //do nothing.
    }

    return res;
  }


}

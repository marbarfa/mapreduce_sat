package main.scala.utils

import java.io.{FileNotFoundException, File}
import main.scala.common.SatMapReduceConstants
import main.scala.domain.{Formula, Clause}
import scala.io.Source

/**
 * Created by marbarfa on 3/2/14.
 */
object SatReader extends ISatReader with SatLoggingUtils{


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
    try{


    for (line <- Source.fromFile(new File(instance_path)).getLines()) {
      //ignore commented lines => starting with # or with the character 'c'
      if (!line.startsWith("#") || line.startsWith("c")) {
        if (line.startsWith("p")){
          //it has a problem definition => initialize.
          var problemDef : Array[String] = line.split(" ")
          clauses = problemDef.apply(problemDef.size-1).toInt
          numberOfVars = problemDef.apply(problemDef.size-2).toInt
        }else{
          var clause = new Clause
          formula.clauses = clause :: formula.clauses
          //read each var of the current clause.
          line.split(" ").foreach(v => {
            try {
              val readVar = Integer.parseInt(v)
              if (readVar != 0){    // ignore 0 literals => should be the last one
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

    }catch {
      case e : Throwable => log.error(s"Error reading sat problem for file: ${instance_path}")
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
  def readSolution() : Boolean = {
    log.debug("Trying to read a 3SAT solution")
    var res = true;
    try{
      res  = Source.fromFile(new File(SatMapReduceConstants.sat_solution_path)).getLines().size > 0
    } catch {
      case e : FileNotFoundException => res = false;
    }

    return res;
  }


}

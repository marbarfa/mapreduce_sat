package main.scala.utils

import java.io.File
import scala.domain.Clause
import scala.io.Source

/**
 * Created by marbarfa on 3/2/14.
 */
object SatReader extends ISatReader {


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
    for (line <- Source.fromFile(new File(instance_path)).getLines()) {
      //ignore commented lines
      if (!line.startsWith("#")) {
        //count clauses and variables.
        clauses += 1;
        var clause = new Clause
        formula.clauses = clause :: formula.clauses
        //read each var of the current clause.
        line.split(" ").foreach(v => {
          try {
            val readVar = Integer.parseInt(v)
            if (math.abs(readVar) > numberOfVars) {
              numberOfVars = Math.abs(readVar)
            }
            clause.literals ::= readVar
            formula.addClauseOfVar(readVar, clause);
          } catch {
            case t: Throwable => {
              println("Error parsing problem instance."); throw new RuntimeException("Error parsing Sat Instance.")
            }
          }
        })
      }
    }
    formula.n = numberOfVars;
    formula.m = clauses;
    return formula;
  }

  /**
   * This method returns true if a solution is found.
   * @return
   */
  def readSolution() : Boolean = Source.fromFile(new File("sat_solution.txt")).getLines().size > 0

}

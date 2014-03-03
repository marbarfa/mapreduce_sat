package scala.utils

import scala.domain.{Variable, Clause, Formula}
import java.io.File
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
        clauses = clauses + 1;
        var vars = line.split(" ");
        var clause = new Clause
        //read each var of the current clause.
        vars.foreach(v => {
          try {
            var readVar = Integer.parseInt(v)
            if (Math.abs(readVar) > numberOfVars) {
              numberOfVars = Math.abs(readVar)
            }
            var variable = new Variable(readVar)
            clause.variables = variable :: clause.variables
          } catch {
            case t: Throwable => {
              println("Error parsing problem instance."); throw new RuntimeException("Error parsing Sat Instance.")
            }
          }
        })
        formula.clauses = clause :: formula.clauses
      }
    }

    return formula;
  }
}

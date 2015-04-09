package utils

import common.SatMapReduceConstants
import domain.{Formula, Clause}
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
    var formula = new Formula

    // read problem instance from file.
    try {
      val fs = FileSystem.get(new Configuration())
      val dataInputStream = fs.open(new Path(instance_path));
      formula.fromCNF(dataInputStream)
      log.info(s"Problem instance read successfully: n=${formula.n}, m=${formula.m}")
      log.info(s"Clauses read: ${formula.clauses.size}")
    } catch {
      case e: Throwable => {
        log.error(s"Error reading sat problem for file: $instance_path", e)
        throw e
      }
    }
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

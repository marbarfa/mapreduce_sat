package common

/**
 * Created by marbarfa on 2/27/14.
 */
object SatMapReduceConstants {
  val zookeper_instance_path: String = "sat_problem_definition"


  val sat_tmp_folder_input = "sat_tmp/input";
  val sat_tmp_folder_output = "sat_tmp/output";

  val sat_solution_path: String = "solution_"

  val sat_not_solution_path: String = "no_solution_"
  val sat_exec_evolution = "sat_execution_"
  val variable_literals_amount = 2

  val HBASE_FORMULA_DEFAULT = "DEFAULT_KEY"

  object config {
    val iteration = "iteration"
    val number_of_mappers = "numbers_of_mappers"
    val depth = "depth"
    val start_miliseconds = "start_miliseconds"
    val problem_path = "problem_path"
    val fixed_literals = "fixed_literals"
    val with_hbase = "with_hbase"
    val execution_start_timestamp = "exec_start_timestamp"
  }

}

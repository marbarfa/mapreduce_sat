package main.scala.common

/**
 * Created by marbarfa on 2/27/14.
 */
object SatMapReduceConstants {
  val zookeper_instance_path: String = "sat_problem_definition"


  val sat_tmp_folder_input = "sat_tmp/input";
  val sat_tmp_folder_output = "sat_tmp/output";

  val sat_solution_path: String = "sol_"

  val sat_not_solution_path: String = "no_sol_"

  val variable_literals_amount = 2

  object config {
    val iteration = "iteration"
    val number_of_mappers = "numbers_of_mappers"
    val depth = "depth"
    val start_miliseconds = "start_miliseconds"
    val problem_path = "problem_path"
    val fixed_literals = "fixed_literals"
  }

}

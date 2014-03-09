package scala.hadoop

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{NLineInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{ToolRunner, Tool}
import scala.common.{SatMapReduceHelper, SatMapReduceConstants}
import scala.utils.{ISatCallback, CacheHelper, SatReader}

/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
class SatMapReduceMain extends Configured with Tool {

  var input : String = _
  var output : String = _
  var iterations : Int = 1

  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {
    input = args(0);
    output = args(1)

    //retrieve problem partition file
    if (args.size != 3) {
      println("Wrong number of inputs. The app needs 2 parameters: \n" +
        "input_path output_path with:\n" +
        "input_path : where input files are located.\n" +
        "output_path: where output files will be saved\n")
    } else {
      var job = createInitJob(input);
      val finishedOk: Boolean = job.waitForCompletion(true)
      if (finishedOk) {
        //Restart job if neccessary.
        if (SatReader.readSolution()) {
          //job ended with solution found!
          return 0
        } else {
          //solution not found yet.
          val job2: Job = createNewJob(output, input + iterations)

        }
      }

    }

    return 1

  }

  private def createInitJob(input: String): Job = {
    var job = createNewJob(input, "it_1");
    FileInputFormat.setInputPaths(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(SatMapReduceConstants.sat_tmp_folder_output))

    var formula = SatReader.read3SatInstance(input);
    var literals: List[Int] = SatMapReduceHelper.generateProblemSplit(List[Int](), formula.n,
      SatMapReduceConstants.variable_literals_amount)

    //generate problem split -> first choose which literals use as variables and how many.
    var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), formula.n, SatMapReduceConstants.variable_literals_amount);

    SatMapReduceHelper.genearteProblemMap(problemSplitVars, new ISatCallback[Map[Int, Boolean]] {
      override def apply(t: Map[Int, Boolean]) =
      //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(t, SatMapReduceConstants.sat_tmp_folder_input + "/init_problem");
    })

    //Upload sat problem to the Zookeeper.
    CacheHelper.putSatInstance(input, getConf)

    return job;
  }

  private def createNewJob(input: String, output: String): Job = {
    val job: Job = new Job(getConf, classOf[SatMapReduceMain].getSimpleName)
    job.setJarByClass(classOf[SatMapReduceMain])
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    //use NLineInputFormat => each mapper will receive one line of the file
    job.setInputFormatClass(classOf[NLineInputFormat]);
  }

  def restartMapReduceJob() {
    //cleanup input path
    //call MapReduce Job.
  }


  def main(args: Array[String]) {
    var res = ToolRunner.run(new Configuration(), new SatMapReduceMain(), args);
    System.exit(res);
  }

}

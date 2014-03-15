package main.scala.hadoop

import main.scala.hadoop.SatJob
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text

import org.apache.hadoop.util.{ToolRunner, Tool}
import scala.common.SatMapReduceHelper
import scala.utils.{ISatCallback, CacheHelper, SatReader}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

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
      var job : SatJob = createInitJob(input);
      var finishedOk: Boolean = job.waitForCompletion(true)
      var end = false;
      while (finishedOk && !end){
        if (SatReader.readSolution()) {
          //job ended with solution found!
          //save solution to output.
          val fs = FileSystem.get(new Configuration())
          fs.rename(new Path(job.output), new Path(output));
          end = true;
        } else {
          //solution not found yet => start next iteration.(input = previous output, output = new tmp
          job = createNewJob(job.output, "tmp_it_" + (job.iteration + 1), job.iteration + 1)
          finishedOk = job.waitForCompletion(true);
        }
      }

      if (finishedOk){
        return 0;
      }else{
        return 1;
      }
    }

    return 1

  }

  private def createInitJob(input: String): SatJob = {
    var job = createNewJob(input, "tmp_it_"+1, 1);

    var formula = SatReader.read3SatInstance(input);

    //generate problem split -> first choose which literals use as variables and how many.
    var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), formula.n, SatMapReduceConstants.variable_literals_amount);

    SatMapReduceHelper.genearteProblemMap(problemSplitVars, new ISatCallback[Map[Int, Boolean]] {
      override def apply(t: Map[Int, Boolean]) =
      //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(t, SatMapReduceConstants.sat_tmp_folder_input + "/init_problem");
    })

    //Upload sat problem to the Zookeeper.
    CacheHelper.putSatInstance(job, input)

    return job;
  }

  private def createNewJob(input: String, output: String, iteration : Int): SatJob = {
    val job = new SatJob(input, output, iteration, getConf, classOf[SatMapReduceMain].getSimpleName)
    job.setJarByClass(classOf[SatMapReduceMain])
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    //use NLineInputFormat => each mapper will receive one line of the file
    job.setInputFormatClass(classOf[NLineInputFormat]);

    FileInputFormat.setInputPaths(job, new Path("in"))
    FileOutputFormat.setOutputPath(job, new Path("out"))

    return job
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

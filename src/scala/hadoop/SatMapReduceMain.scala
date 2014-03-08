package scala.hadoop

import java.enums.EnumSatJobType
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{NLineInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{ToolRunner, Tool}
import scala.common.{SatMapReduceHelper, SatMapReduceConstants}
import scala.utils.{CacheHelper, SatReader}

/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
class SatMapReduceMain extends Configured with Tool {


  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {
    val job: Job = new Job(getConf, classOf[SatMapReduceMain].getSimpleName)

    job.setJarByClass(classOf[SatMapReduceMain])
    job.setMapperClass(classOf[SatMapReduceMapper])

    //retrieve problem partition file
    if (args.size != 4) {
      println("Wrong number of inputs. The app needs 4 parameters: \n" +
        "job_type input_path output_path with:\n" +
        "job_type : {init|rec}\n" +
        "input_path : where input files are located.\n" +
        "output_path: where output files will be saved\n")
    } else {
      var jobType = EnumSatJobType.valueOf(args(0))
      var inputPath = args(1);
      var outputPath = args(2)

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      //use NLineInputFormat => each mapper will receive one line of the file
      job.setInputFormatClass(classOf[NLineInputFormat]);

      FileInputFormat.setInputPaths(job, new Path(SatMapReduceConstants.sat_tmp_folder_input))
      FileOutputFormat.setOutputPath(job, new Path(SatMapReduceConstants.sat_tmp_folder_output))

      if (EnumSatJobType.initial_configuration == jobType) {
        var formula = SatReader.read3SatInstance(inputPath);
        var literals: List[Int] = SatMapReduceHelper.generateProblemSplit(List[Int](), formula.n,
          SatMapReduceConstants.variable_literals_amount)

        //generate problem split -> first choose which literals use as variables and how many.
        var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), formula.n, SatMapReduceConstants.variable_literals_amount);
        //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(List(), problemSplitVars, SatMapReduceConstants.sat_tmp_folder_input + "/init_problem");

        //Upload sat problem to the Zookeeper.
        CacheHelper.putSatInstance(inputPath, getConf)

      } else {
        restartMapReduceJob();
      }

    }

    val finishedOk: Boolean = job.waitForCompletion(true)
    if (finishedOk) return 0
    else return 1

  }

  def restartMapReduceJob() {
    //cleanup input path
    //generate new problem instances.
    //call MapReduce Job.
  }


  def main(args: Array[String]) {
    var res = ToolRunner.run(new Configuration(), new SatMapReduceMain(), args);
    System.exit(res);
  }

}

package scala.hadoop

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{NLineInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import java.enums.EnumSatJobType

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
    val job: Job = new Job(getConf, classOf[SatMapReduceMain].)

    job.setJarByClass(classOf[SatMapReduceMain])
    job.setMapperClass(classOf[SatMapReduceMapper])

    //retrieve problem partition file
    if (args.size != 4){
      println("Wrong number of inputs. The app needs 4 parameters: \n" +
        "job_type input_path output_path with:\n" +
        "job_type : {init|rec}\n" +
        "input_path : where input files are located.\n" +
        "output_path: where output files will be saved\n")
    }else{
      var jobType = EnumSatJobType.valueOf(args(0))
      var inputPath  = args(1);
      var outputPath = args(2)

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[NullWritable])

      //use NLineInputFormat => each mapper will receive one line of the file
      job.setInputFormatClass(classOf[NLineInputFormat]);

      FileInputFormat.setInputPaths(job, new Path(inputPath))
      FileOutputFormat.setOutputPath(job, new Path(outputPath))

      //start mapReduce.


      //when mapReduce finishes.
      //check if solution is found =>
      //if found => finish!.
      //if not found => create new subproblem instances based on previous split
      //                restart MapReduce but now with jobType = iteration


    }

    val finishedOk: Boolean = job.waitForCompletion(true)
    if (finishedOk) return 0
    else return 1

  }

}

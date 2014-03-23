package main.scala.hadoop

import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.utils.{CacheHelper, ISatCallback, SatReader}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{ToolRunner, Tool}


/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
object SatMapReduceMain {

  def main(args: Array[String]) {
    var res = ToolRunner.run(new Configuration(), SatMapReduceJob, args);
    System.exit(res);
  }



}

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMain extends Configured with Tool{


  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {
    val job: Job = new Job(getConf, classOf[SatMapReduceMain].getSimpleName)
    job.setJarByClass(classOf[SatMapReduceMain])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setNumReduceTasks(0)
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    DistributedCache.addCacheFile(FileSystem.get(getConf).makeQualified(new Path("hotlist")).toUri, job.getConfiguration)
    val finishedOk: Boolean = job.waitForCompletion(true)
    if (finishedOk) return 0
    else return 1
  }

}

package hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner


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

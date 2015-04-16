package hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * Created by marbarfa on 3/10/14.
 */
class SatJob(val input: String,
             val output: String,
             val iteration: Int, conf: Configuration,
             name: String) extends Job(conf, name) {

}

package scala.hadoop

import java.io.{IOException, DataInputStream}
import java.net.URI
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.util.bloom.BloomFilter

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[Object, Text, Text, NullWritable ]{



  protected override def setup(context: Mapper#Context) {
    // retrieve 3SAT instance.

    // retrieve blacklist of configurations to prune.
  }

  /**
   * The first map-reduce routine will split the problem saving in the file different configurations.
   * Afterwards, the partitioner will read and give each mapper a line with specific mapper configuration to execute.
   * @param key
   * @param value
   * @param context
   */
  def map(key: Nothing, value: Nothing, context: Nothing) {
    //retrieve current problem split. => the problem will be splitted by the partitoner.


  }
}

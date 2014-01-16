import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.Mapper

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[Object, Text, Text, NullWritable ]{

  def map(key: Nothing, value: Nothing, context: Nothing) {

  }
}

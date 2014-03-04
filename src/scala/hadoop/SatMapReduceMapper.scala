package scala.hadoop

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.domain.Formula
import scala.utils.CacheHelper

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[LongWritable, Text, Text, Text]{
  var formula : Formula = null.asInstanceOf[Formula]


  protected override def setup(context: Mapper#Context) {
    // retrieve 3SAT instance.
    if (formula == null){
      formula = CacheHelper.getSatInstance()
    }

    // retrieve blacklist of configurations to prune.
  }

  /**
   * The mapper key will be
   * @param key line offset
   * @param value
   * @param context
   */
  override def map(key: LongWritable, value: Text, context: Mapper#Context){

  }

}

package algorithms

import org.apache.hadoop.io.{NullWritable, Text, LongWritable}
import org.apache.hadoop.mapreduce.{Reducer, Mapper}

/**
 * Wrapper for MapReduce context.
 * Created by mbarreto on 3/8/15.
 */
class MRContext {

  type ContextMapper = Mapper[LongWritable, Text, LongWritable, Text]#Context
  type ContextReducer = Reducer[LongWritable, Text, NullWritable, Text]#Context

  private var mapperContext : ContextMapper = _
  private var reducerContext : ContextReducer = _

  /**
   * Contructor for Mapper#Context
   * @param ctx
   */
  def MRContext(ctx : ContextMapper) {
      mapperContext = ctx;
  }

  /**
   * Contructor for Reducer#Context
   * @param ctx
   */
  def MRContext(ctx : ContextReducer) {
    reducerContext = ctx;
  }


  def write(longWritable: LongWritable, text: Text): Unit ={
    mapperContext.write(longWritable, text)
  }

  def write(nullWritable: NullWritable, text: Text): Unit ={
    reducerContext.write(nullWritable, text)
  }


}

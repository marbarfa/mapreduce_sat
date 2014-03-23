package main.scala.hadoop

import java.lang
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.domain.Formula
import main.scala.utils.{ConvertionHelper, CacheHelper}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

/**
  * Created by marbarfa on 1/13/14.
  */
class SatMapReduceReducer extends Reducer[Text,Text,NullWritable,Text] with ConvertionHelper{

  var formula : Formula = _
  var table : HTable = _

  override def setup(context: Reducer[Text,Text,NullWritable,Text]#Context) {
    // retrieve 3SAT instance.
    if (formula == null) {
      formula = CacheHelper.sat_instance(context.getConfiguration.get("problem_path"))
    }

    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  def saveSolution(solutionMap: Map[Int, Boolean]) = {
    //save solution to file.
    SatMapReduceHelper.saveProblemSplit(solutionMap, SatMapReduceConstants.sat_solution_path)

  }

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text,Text,NullWritable,Text]#Context) {
       values.asScala.foreach(v => {
         var literalDefinition = SatMapReduceHelper.parseInstanceDef(key.toString + " " + v.toString)
          if (formula.n == literalDefinition.keySet.size){
            //all literals are set.
            if (formula.isSatisfasiable(literalDefinition)){
              saveSolution(literalDefinition);
            }
          }else{
            //still a partial solution
            context.write(NullWritable.get(), new Text(SatMapReduceHelper.createSatString(literalDefinition).getBytes))
          }
       })
  }


 }

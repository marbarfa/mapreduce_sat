package scala.hadoop

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.domain.{Variable, Formula}
import scala.utils.CacheHelper

/**
 * Created by marbarfa on 1/13/14.
 */
class SatMapReduceMapper extends Mapper[LongWritable, Text, Text, Text]{
  var formula : Formula = null.asInstanceOf[Formula]

  var table : HTable = null

  protected override def setup(context: Mapper#Context) {
    // retrieve 3SAT instance.
    if (formula == null){
      formula = CacheHelper.getSatInstance()
    }

    // Get HBase table of invalid variable combination
    val hconf = HBaseConfiguration.create
    table = new HTable(hconf, "var_tables")
  }

  /**
   * The mapper key will be
   * @param key line offset
   * @param value subproblem definition like: (1 -2;3 4) --> fixed values: 1=true and 2=false and
   *              2 variables: (3=false,4=false)
   *@param context
   */
  override def map(key: LongWritable, value: Text, context: Mapper#Context){
      var d = CacheHelper.getDepth();
      var vars = value.toString.split(" ").toList.map(x => new Variable(x.toInt))
      var possibleVars= getPossibleVar(vars, d)



  }


  private def getPossibleVar(vars : List[Variable], d : Int) : List[Variable] = {
    var res = List[Variable]()
    var v = getVar(vars);
    while (v != null && res.size < d){
      res = v :: res;
      v = getVar(vars ::: res);
    }

    return res;
  }

  private def getVar(vars : List[Variable]) : Variable = {
    for(i <-  0 to formula.n) vars.foreach(v => if (v.number != i) return new Variable(number = i))
    return null;
  }

}

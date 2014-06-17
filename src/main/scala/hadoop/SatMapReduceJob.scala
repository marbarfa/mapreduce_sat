package main.scala.hadoop

import java.util.Date
import main.java.enums.EnumMRCounters
import main.scala.common.{SatMapReduceConstants, SatMapReduceHelper}
import main.scala.utils.{SatLoggingUtils, CacheHelper, ISatCallback, SatReader}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Delete, Scan, ResultScanner, HTable, Result}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import scala.collection.JavaConverters._


/**
 * Created by marbarfa on 1/13/14.
 * Main MapReduce program. This program is the main job for the MapReduce SAT solver.
 */
object SatMapReduceJob extends Configured with Tool with SatLoggingUtils {

  var instance_path : String = _
  var startTime : Long = _
  var numberOfMappers : Int = _
  var numberOfLiterals : Int = _

  /**
   * Main SAT program.
   * @param args
   * @return
   */
  def run(args: Array[String]): Int = {

    //retrieve problem partition file
    if (args.size != 2) {
      log.info(
        s"""
           |Wrong number of inputs. The app needs 2 parameters:
           |input_path number_of_splits with:
           |input_path : where input files are located.
           |number of splits: how many mappers to create in each iteration.
         """.stripMargin
        )
    } else {

      log.info("Starting mapreduce algorithm...")
      log.info("Cleaning previous information...")
      cleanup()

      startTime = System.currentTimeMillis();
      instance_path = args(0)
      numberOfMappers = args(1).toInt

      var job : SatJob = createInitJob(instance_path, 3);

      var finishedOk: Boolean = job.waitForCompletion(true)
      var end = false;
      while (finishedOk && !end){
        log.info(s"#################################################################################################")
        var solCounter = job.getCounters.findCounter(EnumMRCounters.SOLUTIONS)
        if (SatReader.readSolution(instance_path) || solCounter != null && solCounter.getValue > 0) {
          log.info(s"Solution file found!, found ${solCounter.getValue} solutions..., finishing algorithm....")
          end = true;
        } else {
          log.info(s"#################################################################################################")
          log.info (s"####################Solution not found, starting iteration ${job.iteration + 1}################")
          log.info(s"#################################################################################################")
          //solution not found yet => start next iteration.(input = previous output, output = new tmp
          var fixedLit = job.getConfiguration.getInt("fixed_literals", 0);
          log.info(s"#####Fixed literals configuration: $fixedLit")

          if (fixedLit < numberOfLiterals){
            job = createNewJob(job.output,
              SatMapReduceConstants.sat_tmp_folder_output + "_" + (job.iteration + 1),
              job.iteration + 1,
              fixedLit + numberOfMappers)

            finishedOk = job.waitForCompletion(true);
            var tasks = job.getConfiguration.get("mapreduce.job.maps");
            log.info(s"Job tasks $tasks")
            if (tasks != null && tasks.equals("0")){
              log.info(s"Finishing MR job without solutions!.")
              end = true;
            }
          }else{
            end = true;
          }
        }
      }

      if (finishedOk){
        return 0;
      }else{
        return 1;
      }
    }

    return 1

  }


  private def cleanup(){
    //delete existing output folders.
    log.info("Deleting output folders...")
    try {
      val fs = FileSystem.get(new Configuration())
      fs.delete(new Path("sat_tmp"), true);
    } catch {
      case e : Throwable  => //do nothing in all cases.
    }

    //cleanup DB.
    log.info("Cleaning up database...")
    val hconf = HBaseConfiguration.create
    var hTable = new HTable(hconf, "var_tables")
    var scanner : ResultScanner = hTable.getScanner(new Scan());

    for (result : Result <- scanner.asScala) {
      var delete = new Delete(result.getRow);
      hTable.delete(delete);
    }

  }

  private def createInitJob(input: String, numberOfMappers : Int): SatJob = {
    var time = new Date()
    var input_path = SatMapReduceConstants.sat_tmp_folder_input + s"_${time.getTime}"
    var job = createNewJob(input_path, SatMapReduceConstants.sat_tmp_folder_output + "_1", 1, numberOfMappers);

    var formula = SatReader.read3SatInstance(input);
    numberOfLiterals = formula.n;

    //generate problem split -> first choose which literals use as variables and how many.
    var problemSplitVars = SatMapReduceHelper.generateProblemSplit(List(), numberOfMappers, formula);
    SatMapReduceHelper.genearteProblemMap(problemSplitVars, new ISatCallback[List[Int]] {
      override def apply(t: List[Int]) =
      //save problem definition in the input path to be used as input in the MapReduce algorithm.
        SatMapReduceHelper.saveProblemSplit(t, input_path);
    })

    job.getConfiguration().setInt("fixed_literals", numberOfMappers);
    log.info(s"########JOB#####fixed: ${numberOfMappers}")
    return job;
  }



  private def createNewJob(input: String, output: String, iteration : Int, fixedLiterals : Int): SatJob = {
    val job = new SatJob(input, output, iteration, getConf, SatMapReduceJob.getClass.getSimpleName)

    job.setJarByClass(SatMapReduceJob.getClass)
    job.setMapperClass(classOf[SatMapReduceMapper])
    job.setReducerClass(classOf[SatMapReduceReducer])


    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.getConfiguration.set("iteration", iteration.toString);
    job.getConfiguration.set("numbers_of_mappers", numberOfMappers.toString);
    job.getConfiguration.set("start_miliseconds", startTime.toString);
    job.getConfiguration.set("problem_path", instance_path);
    job.getConfiguration.setInt("fixed_literals", fixedLiterals);
//    job.getCounters.addGroup("EnumMRCounters", "SOLUTIONS")

    //use NLineInputFormat => each mapper will receive one line of the file

    FileInputFormat.setInputPaths(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    job.setInputFormatClass(classOf[NLineInputFormat]);

    //Upload sat problem to the Zookeeper.
    CacheHelper.putSatInstance(job, instance_path)
    return job
  }

}

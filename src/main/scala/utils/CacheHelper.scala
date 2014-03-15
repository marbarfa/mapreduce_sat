package main.scala.utils

import java.net.URI
import org.apache.hadoop.mapreduce.Job

/**
 * Created by marbarfa on 3/3/14.
 */
object CacheHelper {

  /**
   * Uploads the SAT instance to the Zookeeper
   * @return
   */
  def putSatInstance(job : Job, problem_path : String) = {
    job.addCacheArchive(new URI(problem_path))
  }


  /**
   * Retrieves the SAT instance from Distributed Cache.
   * @return
   */
  def sat_instance : Formula = SatReader.read3SatInstance(SatMapReduceConstants.zookeper_instance_path)

  def depth : Int = 4

}

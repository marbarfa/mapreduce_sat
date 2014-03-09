package scala.utils

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import scala.common.SatMapReduceConstants
import scala.domain.Formula

/**
 * Created by marbarfa on 3/3/14.
 */
object CacheHelper {

  /**
   * Uploads the SAT instance to the Zookeeper
   * @return
   */
  def putSatInstance(problem_path : String, conf : Configuration) = {
    DistributedCache.addCacheArchive(new URI(problem_path), conf)
  }


  /**
   * Retrieves the SAT instance from Distributed Cache.
   * @return
   */
  def sat_instance : Formula = SatReader.read3SatInstance(SatMapReduceConstants.zookeper_instance_path)

  def depth : Int = 4

  def


}

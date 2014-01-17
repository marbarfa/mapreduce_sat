import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BloomFilteringMain extends Configured implements Tool{
    public int run(String[] args) throws Exception
    {
    	/**
    	 * CHECK PAGE 227 OF THE BOOK. THIS SHIT OF DISTRIBUTED CACHE IS NOT WORKING.
    	 */
    	
        //creating a JobConf object and assigning a job name for identification purposes
        Job job = new Job(getConf(), BloomFilteringMain.class.getSimpleName());
//        conf.setJobName("WordCount");
        job.setJarByClass(BloomFilteringMain.class);
        //Setting configuration object with the Data Type of output Key and Value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(BloomFilteringMapper.class);
        job.setNumReduceTasks(0);
        
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        DistributedCache.addCacheFile(
				FileSystem.get(getConf()).makeQualified(new Path("hotlist"))
						.toUri(), job.getConfiguration());
		// Read into our Bloom filter.
        
        boolean finishedOk = job.waitForCompletion(true);
        
        if (finishedOk)
        	return 0;
        else
        	return 1;
    }
    
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new BloomFilteringMain(),args);
        System.exit(res);
    }
    
    
}
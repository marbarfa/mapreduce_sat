import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class BloomFilteringMapper extends
		Mapper<Object, Text, Text, NullWritable> {

	private static BloomFilter filter;

	// SETUP CODE.
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		filter = new BloomFilter();
		
		URI[] files = DistributedCache.getCacheFiles(context
				.getConfiguration());
		FileSystem fs = FileSystem.get(context.getConfiguration());
		// if the files in the distributed cache are set
		if (files != null && files.length == 1) {
			System.out.println("Reading Bloom filter from: "
					+ files[0].getPath());

			// Open local file for read.
			DataInputStream strm = new DataInputStream(fs.open(new Path(files[0].getPath())));

			// Read into our Bloom filter.
			filter.readFields(strm);
			strm.close();
		} else {
			throw new IOException(
					"Bloom filter file not set in the DistributedCache.");
		}
		// Get file from the DistributedCache
        // Open local file for read.
//		FileSystem fs = FileSystem.get(context.getConfiguration());
//		
//		DataInputStream strm = new DataInputStream(fs.open(new Path("hotlist")));
//        // Read into our Bloom filter.
//        filter.readFields(strm);
//        strm.close();
	}

	// MAPPER CODE
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
				.toString());

		// Grab the necessary XML attributes
		String comment = parsed.get("Body");
		// String txt = parsed.get("Body");
		String posttype = parsed.get("PostTypeId");
		String row_id = parsed.get("Id");

		// if the body is null, or the post is a question (1), skip
		if (comment == null) {
			return;
		}
		comment = StringEscapeUtils.unescapeHtml(comment.toLowerCase());

		// For each word in the comment
		StringTokenizer tokenizer = new StringTokenizer(comment);
		while (tokenizer.hasMoreTokens()) {
			// If the word is in the filter, output the record and break
			String word = tokenizer.nextToken();
			if (filter.membershipTest(new Key(word.getBytes()))) {
				context.write(new Text(value), NullWritable.get());
				break;
			}
		}
	}
	
	
	
}

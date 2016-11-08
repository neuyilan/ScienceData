package ict.science.dealdata.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExtractRowMR {

	// private static final Log LOG = LogFactory.getLog(ExtractRowMR.class);

	public static class ExtractMap extends Mapper<Object, Text, Text, Text> {
		private static String SEPARATOR = ",";

		// the software number attribute
		private static String SOFTNO = "softNo";

		// the software number attribute value, this is only a fake value, for
		// test
		private static String SOFTNOVALUE = "0";

		private static String[] title = { "entry", "runNo", "eventId",
				"totalCharged", "totalNeutral", "totalTrks" };
		private Text resultKey = new Text();
		private Text resultValue = new Text();

		// use StringTokenizer
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String item[] = new String[6];
			String tmpKey = null;
			int i = 0;
			StringTokenizer itr = new StringTokenizer(value.toString(),
					SEPARATOR);
			while (itr.hasMoreTokens()) {
				item[i] = itr.nextToken();
				i++;
			}
			for (i = 3; i < item.length; i++) {
				tmpKey = SOFTNO + "#" + SOFTNOVALUE + "#" + "runNo" + "#"
						+ item[1] + "#" + title[i] + "#" + item[i];
				resultKey.set(tmpKey);
				resultValue.set(item[2]);
				context.write(resultKey, resultValue);
			}

		}

		/**
		 *  use java string split
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String item[] = null;
			String tmpKey = null;

			item = value.toString().split(SEPARATOR);
			for (int i = 3; i < item.length; i++) {
				tmpKey = SOFTNO + "#" + SOFTNOVALUE + "#" + "runNo" + "#"
						+ item[1] + "#" + title[i] + "#" + item[i];
				resultKey.set(tmpKey);
				resultValue.set(item[2]);
				context.write(resultKey, resultValue);
				LOG.info("**************2" + System.currentTimeMillis());
			}
		}
		**/

	}

	public static class ExtractReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text resultValue = new Text();
			StringBuilder tempValue = new StringBuilder();
			for (Text value : values) {
				tempValue.append(value.toString()).append("#");
			}
			resultValue.set(tempValue.subSequence(1, tempValue.length() - 1)
					.toString());
			context.write(key, resultValue);
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: ExtractRowMR <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "extract row job");
		job.setJarByClass(ExtractRowMR.class);
		job.setMapperClass(ExtractMap.class);
		job.setCombinerClass(ExtractReduce.class);
		job.setReducerClass(ExtractReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		// delete the output path if exists
		Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
		FileSystem hdfs = FileSystem.newInstance(conf);
		if (null != hdfs) {
			if (hdfs.exists(outputPath)) {
				hdfs.delete(outputPath, true);
			}
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

package ict.science.dealdata.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExtractRowMR {

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
				context.write(new Text(tmpKey), new Text(item[2]));
			}
		}
	}

	public static class ExtractReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text resultValue = new Text();
			String tempValue="#";
			for (Text value : values) {
				tempValue += value.toString()+"#";
			}
			tempValue=tempValue.substring(1, tempValue.length()-1);
			resultValue.set(tempValue);
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
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

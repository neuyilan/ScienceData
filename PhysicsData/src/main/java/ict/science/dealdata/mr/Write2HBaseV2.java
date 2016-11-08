package ict.science.dealdata.mr;

import ict.science.dealdata.util.HFileLoader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Write2HBaseV2 {
	private static String token = "\t"; 
	private static final byte[] FAMILY_BYTE=Bytes.toBytes("cf");
	private static final byte[] QUALIFIER_INDEX=Bytes.toBytes("event");
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: <table name> <in>  <out>");
			System.err.println("for example: test_table_tag /file_input_path  /file_output_path");
			System.exit(2);
		}
		
		String tableNameStr = args[0];
		String inputPathStr = args[1];
		String outputPathStr = args[2];
		conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseConfiguration.addHbaseResources(conf);
		
		Job job = Job.getInstance(conf,"load_data_to_"+tableNameStr);
		job.setJarByClass(Write2HBaseV2.class);
		
		job.setMapperClass(generateHFileMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		//delete the output path if exists
		Path inputPath = new Path(inputPathStr);
		Path outputPath = new Path(outputPathStr);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileSystem hdfs = FileSystem.newInstance(conf);
		if(null!=hdfs){
			if(hdfs.exists(outputPath)){
				hdfs.delete(outputPath, true);
			}
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		Connection conn = ConnectionFactory.createConnection(conf);
		TableName tableName = TableName.valueOf(tableNameStr);
		Table table = conn.getTable(tableName);
		HFileOutputFormat2.configureIncrementalLoad(job,table,conn.getRegionLocator(tableName));
		
		job.waitForCompletion(true);
		
		if(job.isSuccessful()){
			HFileLoader.doBulkload(outputPath,tableNameStr); //import the data(hfile) to hbase
		}
		
	}
	
	public  static class  generateHFileMapper extends Mapper <LongWritable, Text, ImmutableBytesWritable, Put> {
		String temp[];
		ImmutableBytesWritable rowkey  = new ImmutableBytesWritable();
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			temp = value.toString().split(token);
			if(temp.length!=2){
				System.err.println("wrong length about the input file for the record:"+value.toString());
			}else{
				rowkey.set(Bytes.toBytes(temp[0]));
				Put put = new Put(rowkey.copyBytes());
				put.addColumn(FAMILY_BYTE,QUALIFIER_INDEX,Bytes.toBytes(temp[1]));
				context.write(rowkey, put);
			}
		}
	}

}

package ict.science.dealdata.mr;

import ict.science.dealdata.util.HFileLoader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Import2HBaseOldSchema {

	
	private static String cf="cf";
	private static String token = ","; 
	private static String qualifier="EMPTY";

	
	public static void main(String args[]){
		if (args.length != 3) {
			System.err.println("Usage: <table name> <in>  <out>");
			System.err.println("for example: test_table_tag /file_input_path  /file_output_path");
			System.exit(2);
		}
		String tableNameStr = args[0];
		String inputPathStr = args[1];
		String outputPathStr = args[2];
		
		try {
			createJob(tableNameStr,inputPathStr,outputPathStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void createJob(String tableNameStr,String inputPathStr,String outputPathStr) throws Exception{
		Configuration conf = new Configuration();
		Path inputPath = new Path(inputPathStr);
		Path outputPath = new Path(outputPathStr);
		
		conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseConfiguration.addHbaseResources(conf);
		
		Job job = Job.getInstance(conf,"import_data_to_"+tableNameStr);
		job.setJarByClass(Write2HBase.class);
		
		FileInputFormat.addInputPath(job,inputPath);
		
		
		job.setMapperClass(generateHFileMapper.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(KeyValue.class);
		
		//delete the output path if exists
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
	
	
	public static class  generateHFileMapper extends Mapper <Object, Text, ImmutableBytesWritable, KeyValue> {
		String temp[];
		ImmutableBytesWritable rowkey  = new ImmutableBytesWritable();
		KeyValue kv = null;
		String tmpKey = null;
		String attr = null;
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			temp = value.toString().split(token);
			if(temp.length!=6){
				System.err.println("wrong length about the input file for the record:"+value.toString());
			}else{
				for(int i =3;i<temp.length;i++){
					if(i==3){
						attr = "totalCharged";
					}else if(i==4){
						attr = "totalNeutral";
					}else if(i==5){
						attr = "totalTrks";
					}
					tmpKey = "softNo#0#runNo#"+temp[1]+"#"+attr+"#"+temp[i]+"#"+temp[2];
					rowkey.set(Bytes.toBytes(tmpKey));
					kv = new KeyValue(Bytes.toBytes(tmpKey),Bytes.toBytes(cf),Bytes.toBytes(qualifier),Bytes.toBytes(temp[2]));
					if(null !=kv){
						context.write(rowkey, kv);
					}
				}
			}
		}
	}
}

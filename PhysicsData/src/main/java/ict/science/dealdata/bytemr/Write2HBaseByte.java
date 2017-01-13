package ict.science.dealdata.bytemr;

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

public class Write2HBaseByte {
	private static String cf="cf";
	private static String token = "\t"; 
	private static String qualifier="event";

	
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
		
		Job job = Job.getInstance(conf,"load_data_to_"+tableNameStr);
		job.setJarByClass(Write2HBaseByte.class);
		
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
		
		byte [] byteRow;
		String attributeValue; // the attribute value, Integer,Double 
		String rowKeyPartArray[];
		
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			temp = value.toString().split(token);
			if(temp.length!=2){
				System.out.println("***************************************wrong length about the input file for the record:"+value.toString());
			}else{
				rowkey = generateRowKey(temp[0]);
				kv = new KeyValue(byteRow,Bytes.toBytes(cf),Bytes.toBytes(qualifier),Bytes.toBytes(temp[1]));
			}
			if(null!=kv){
				context.write(rowkey, kv);
			}
		}
		
		
		public ImmutableBytesWritable generateRowKey(String rowKeyPart){
			String newRowPrefix="";
			rowKeyPartArray = rowKeyPart.split("#");
			attributeValue=rowKeyPartArray[rowKeyPartArray.length-1];
			for(int i =0;i<rowKeyPartArray.length-1;i++){
				newRowPrefix+=rowKeyPartArray[i]+"#";
			}
//			newRowPrefix = newRowPrefix.substring(0, newRowPrefix.length()-1);
			byteRow = generateByteRow(newRowPrefix,Integer.parseInt(attributeValue));
			
			rowkey.set(byteRow, 0, byteRow.length);
			
			return rowkey;
		}
		
		
		
		public byte[] generateByteRow(String prefix,int suffix){
			byte[] key_prefix = Bytes.toBytes(prefix);
			byte[] key_suffix = Bytes.toBytes(suffix);
			byte[] key_all = new byte[key_prefix.length+key_suffix.length];
			
			System.arraycopy(key_prefix, 0, key_all, 0, key_prefix.length);
			System.arraycopy(key_suffix, 0, key_all, key_prefix.length, key_suffix.length);
			return key_all;
		}
		
	}

}

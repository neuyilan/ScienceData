package ict.science.dealdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HFileLoader {
	
	public static void doBulkload(Path hfilePath ,String tableNameStr){
		try{
			Configuration conf =HBaseConfiguration.create();
			conf.set("fs.defaultFS", "hdfs://data18:9000");
			conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
			HBaseConfiguration.addHbaseResources(conf);
			LoadIncrementalHFiles loadFile = new LoadIncrementalHFiles(conf);
			
			Connection conn = ConnectionFactory.createConnection(conf);
			TableName tableName = TableName.valueOf(tableNameStr);
			Table table = conn.getTable(tableName);
			Admin admin = conn.getAdmin();
			loadFile.doBulkLoad(hfilePath, admin, table, conn.getRegionLocator(tableName));
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
}

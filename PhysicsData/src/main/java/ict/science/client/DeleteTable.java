package ict.science.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DeleteTable {

	private static String tableNameStr="TAG_PP";
	private static Configuration conf;
	private static Admin admin;
	private static Connection conn;
	
	
	public static void main(String[] args) throws IOException{
		if(args.length==2){
			tableNameStr = args[0];
		}
		System.out.println("tableName ------->"+tableNameStr);
		
		conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conn = ConnectionFactory.createConnection(conf);
		admin = conn.getAdmin();
		
		TableName tableName = TableName.valueOf(tableNameStr);
		
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		
		System.out.println("delete table finished!");
		return;
	}
}

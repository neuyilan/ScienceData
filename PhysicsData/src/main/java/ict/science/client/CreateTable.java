package ict.science.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {
	private static String tableNameStr="test_table";
	private static Configuration conf;
	private static Admin admin;
	private static HTableDescriptor desc;
	private static Connection conn;
	private static String columnfamily = "cf";
	
	
	public static void main(String[] args) throws IOException{
		if(args.length==2){
			tableNameStr = args[0];
			columnfamily=args[1];
		}
		System.out.println("tableName ------->"+tableNameStr);
		System.out.println("columnfamily ------->"+columnfamily);
		
		conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conn = ConnectionFactory.createConnection(conf);
		admin = conn.getAdmin();
		
		TableName tableName = TableName.valueOf(tableNameStr);
		
		desc = new HTableDescriptor(tableName);

		HColumnDescriptor h1 = new HColumnDescriptor(Bytes.toBytes(columnfamily));
		h1.setMaxVersions(10);
		h1.setMinVersions(3);
		desc.addFamily(h1);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		admin.createTable(desc);
		System.out.println("Create table finished!");
		return;
	}

}

package ict.science.dealdata.bytemr;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTableByte {
	private static String tableNameStr="test_table";
	private static Configuration conf;
	private static Admin admin;
	private static HTableDescriptor desc;
	private static Connection conn;
	private static String columnfamily = "cf";
	private static Table table;
	private static TableName tableName;
	
	
	
	public static void main(String[] args) throws IOException{
		init();
//		scanTest();
		createTable();
		loadData();
	}
	
	public static void loadData() throws IOException{
		table  = conn.getTable(tableName);
		
		for (int i = 0; i < 100; i++) {
			
			String tmpRowKey = "tmp#";
			byte[] key_start;
			byte[] key_end;
//			double iv = i+200.0;
			Random random   = new Random();
			int iv = random.nextInt(1000)-random.nextInt(10000);
			
			if(iv<0){
				key_start = Bytes.toBytes(tmpRowKey+"@");
				key_end= Bytes.toBytes(iv);
			}else{
				key_start = Bytes.toBytes(tmpRowKey);
				key_end= Bytes.toBytes(iv);
			}
			
			byte[] key_all = new byte[key_start.length+key_end.length];
			System.arraycopy(key_start, 0, key_all, 0, key_start.length);
			System.arraycopy(key_end, 0, key_all, key_start.length, key_end.length);
			
//			Put p = new Put(key_all);
			Put p = new Put(key_end);
			long value = i;
			
			p.addColumn(Bytes.toBytes(columnfamily),
					Bytes.toBytes("test"),
					Bytes.toBytes("va"+(iv)));
			table.put(p);
			
			
//			// float 
//			Random random   = new Random();
//			double dv = random.nextDouble()*1000-random.nextDouble()*10000;
//			byte[] key_start_f;
//			byte[] key_end_f;
//			
//			if(dv<0){
////				key_start_f = Bytes.toBytes(tmpRowKey+"@");
//				key_end_f = Bytes.toBytes(dv);
//			}else{
////				key_start_f = Bytes.toBytes(tmpRowKey);
//				key_end_f = Bytes.toBytes(dv);
//			}
//			
////			byte[] key_all_f = new byte[key_start_f.length+key_end_f.length];
////			System.arraycopy(key_start_f, 0, key_all_f, 0, key_start_f.length);
////			System.arraycopy(key_end_f, 0, key_all_f, key_start_f.length, key_end_f.length);
////			p = new Put(key_all_f);
//			Put p = new Put(key_end_f);
//			
//			p.addColumn(Bytes.toBytes(columnfamily),
//					Bytes.toBytes("test"),
//					Bytes.toBytes("va"+(dv)));
//			table.put(p);
		}
	}
	
	
	
	public static void init() throws IOException{
		conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conn = ConnectionFactory.createConnection(conf);
		admin = conn.getAdmin();
		tableName = TableName.valueOf(tableNameStr);
		table  = conn.getTable(tableName);
	}
	
	public static void createTable() throws IOException{
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
	}
	
	
	public static void scanTest() throws IOException {
		Scan scan = new Scan();
		
		String startRowStr = "tmp_";
		String endRowStr = "tmp_";
		double satrt_d = -10;
		double end_d=10.0;
		byte[] start_byte_row_pre = Bytes.toBytes(startRowStr);
		byte[] start_byte_row_suf = Bytes.toBytes(satrt_d);
		
		byte[] end_byte_row_pre = Bytes.toBytes(endRowStr);
		byte[] end_byte_row_suf = Bytes.toBytes(end_d);
		
		byte[] start_key = new byte[start_byte_row_pre.length+start_byte_row_suf.length];
		System.arraycopy(start_byte_row_pre, 0, start_key, 0, start_byte_row_pre.length);
		System.arraycopy(start_byte_row_suf, 0, start_key, start_byte_row_pre.length, start_byte_row_suf.length);
		
		byte[] end_key = new byte[end_byte_row_pre.length+end_byte_row_suf.length];
		System.arraycopy(end_byte_row_pre, 0, end_key, 0, end_byte_row_pre.length);
		System.arraycopy(end_byte_row_suf, 0, end_key, end_byte_row_pre.length, end_byte_row_suf.length);
		
//		scan.setStartRow(start_key);
//		scan.setStopRow(end_key);
		
		scan.setStartRow(Bytes.toBytes(satrt_d));
		scan.setStopRow(Bytes.toBytes(end_d));
		
		
		ResultScanner scanner = table.getScanner(scan);
		Result res ;
		while ((res= scanner.next())!= null) {
			println(res);
		}
	}
	
	static void println(Result result) {
		List<Cell> cellList = result.listCells();
		for (Cell cell : cellList) {
			 System.out
			 .println(CellUtil.cloneRow(cell) + "\t"
			 + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
			 + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
			 + Bytes.toString(CellUtil.cloneValue(cell)));
//			System.out
//			.println("*********"+Bytes.toString(CellUtil.cloneRow(cell)) );
		}

	}
}

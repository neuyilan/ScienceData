package ict.science.dealdata.bytemr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanFromHbaseByte {

	private static Configuration conf;
	private static String tableNameStr = "byte_test_table";

	private static TableName tableName;
	private static Table table;
	private static Admin admin;
	private static Connection conn;
	
	static String path = "/home/qhl/physicsData/data/";
	static String rFileName = "query.sql";
	static String wFileName = "query_hbase.csv";
	

	public static void main(String[] args) throws IOException {
		init();
		
		scanTest();
		
		close();
	}
	
	public static void close(){
		try {
			table.close();
			admin.close();
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public static void init() {
		conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://data18:9000");
		conf.set("hbase.zookeeper.quorum", "data17,data19,data20");
		HBaseConfiguration.addHbaseResources(conf);
		try {
			conn = ConnectionFactory.createConnection(conf);
			tableName = TableName.valueOf(tableNameStr);
			table = conn.getTable(tableName);
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void scanTest() throws IOException {
		

		Scan scan = new Scan();
		String runNoValue = null;
		String attr = null;
		String attrValue = null;
		

		
		File rfile = new File(path + rFileName);
		FileReader fr;
		BufferedReader br ;
		
		File wfile = new File(path + wFileName);
		FileWriter fw;
		BufferedWriter bw;
		
		
		fr = new FileReader(rfile);
		br = new BufferedReader(fr);
		
		fw = new FileWriter(wfile);
		bw = new BufferedWriter(fw);
		

		String tempSql;
		
		int numCount = 0;
		
		while ((tempSql = br.readLine()) != null) {
			numCount++;
//			if(numCount>100){
//				break;
//			}
			
			int count = 0;
			long startTime = 0;
			long endTime = 0;
			
			runNoValue = tempSql.substring(40, 45);
			if(tempSql.contains("totalCharged")){
				attr = "totalCharged";
				attrValue = tempSql.substring(66, tempSql.length()-1);
			}else if(tempSql.contains("totalNeutral")){
				attr = "totalNeutral";
				attrValue = tempSql.substring(66, tempSql.length()-1);
			}else{
				attr = "totalTrks";
				attrValue = tempSql.substring(63, tempSql.length()-1);
			}
			
			String startRowStr = "softNo#0#runNo#" + runNoValue + "#" + attr + "#";
			String endRowStr = "softNo#0#runNo#" + runNoValue + "#" + attr + "#";
			System.out.println("**************"+attrValue);
			byte[] start_byte_row_pre = Bytes.toBytes(startRowStr);
			byte[] start_byte_row_suf = Bytes.toBytes(Integer.parseInt(attrValue));
			
			byte[] end_byte_row_pre = Bytes.toBytes(endRowStr);
			byte[] end_byte_row_suf = Bytes.toBytes(Integer.parseInt(attrValue)+10);
			
			byte[] start_key = new byte[start_byte_row_pre.length+start_byte_row_suf.length];
			System.arraycopy(start_byte_row_pre, 0, start_key, 0, start_byte_row_pre.length);
			System.arraycopy(start_byte_row_suf, 0, start_key, start_byte_row_pre.length, start_byte_row_suf.length);
			
			byte[] end_key = new byte[end_byte_row_pre.length+end_byte_row_suf.length];
			System.arraycopy(end_byte_row_pre, 0, end_key, 0, end_byte_row_pre.length);
			System.arraycopy(end_byte_row_suf, 0, end_key, end_byte_row_pre.length, end_byte_row_suf.length);
			
			
			
			System.out.println(startRowStr);
			System.out.println(endRowStr);
			
			
			
			
			scan.setStartRow(start_key);
			scan.setStopRow(end_key);

			ResultScanner scanner = table.getScanner(scan);
			Result res ;
			startTime = System.currentTimeMillis();
			while ((res= scanner.next())!= null) {
				println(res);
				count =getCount(res);
			}
			endTime = System.currentTimeMillis();
			
			bw.write(tempSql+","+(endTime-startTime)+","+count+"\n");

//			System.out.println("total times cost:" + (stopTime - startTime)
//					+ " ms");
//			System.out.println("total row number: " + count);
		}

		bw.close();
		br.close();
	}

	
	private static int getCount(Result result){
		List<Cell> cellList = result.listCells();
		int  count = 0;
		for (Cell cell : cellList) {
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			count =  value.split("#").length;
		}
		return count;
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

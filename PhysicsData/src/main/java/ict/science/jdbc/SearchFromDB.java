package ict.science.jdbc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class SearchFromDB {
	
	static DBHelper db1 = null;
	static ResultSet ret = null;
	static String tableName = "tag2";
	static PreparedStatement pst = null;
	static String path = "/home/qhl/physicsData/data/";
	static String rFileName = "query.sql";
	static String wFileName = "query_mysql_index.csv";

	
	public static void main(String[] args) {
		search();
	}
	
	public static void search() {
		File rfile = new File(path + rFileName);
		FileReader fr;
		BufferedReader br ;
		
		File wfile = new File(path + wFileName);
		FileWriter fw;
		BufferedWriter bw;
		
		
		try {
			fr = new FileReader(rfile);
			br = new BufferedReader(fr);
			
			fw = new FileWriter(wfile);
			bw = new BufferedWriter(fw);
			
			db1 = new DBHelper();
			Connection conn = db1.getConn();
			
			String tempSql = null;
			while ((tempSql = br.readLine()) != null) {
				int count = 0;
				long startTime = System.currentTimeMillis();
				try {
					pst = conn.prepareStatement(tempSql);//准备执行语句  
					ret = pst.executeQuery();// 执行语句，得到结果集
					while (ret.next()) {
						count++;
					}
					ret.close();
					long endTime = System.currentTimeMillis();
					bw.write(tempSql+","+(endTime-startTime)+","+count+"\n");
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			db1.close();
			br.close();
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static void test(){
		String sql = null;
		sql = "select count(*) from tag2";
//		sql = "select eventId from tag2  where runNo= '-8291' and totalCharged ='3'";// SQL语句
		db1 = new DBHelper();// 创建DBHelper对象
		Connection conn = db1.getConn();
		try {
			pst = conn.prepareStatement(sql);//准备执行语句  
			ret = pst.executeQuery();// 执行语句，得到结果集
			while (ret.next()) {
				String eventId = ret.getString(1);
				System.out.println(eventId);

			}// 显示数据
			ret.close();
			db1.close();// 关闭连接
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

}
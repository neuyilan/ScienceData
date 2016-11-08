package ict.science.phoenix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SearchFromPhoenix {

	static String path = "/home/qhl/physicsData/data/";
	static String rFileName = "query.sql";
	static String wFileName = "query_phoenix.csv";

	static Connection con;
	static Statement stmt = null;
	static ResultSet rset = null;

	public static void main(String[] args) throws SQLException {

		init();
		try {
			search();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void init() {
		try {
			con = DriverManager.getConnection("jdbc:phoenix:data19:2181");
			stmt = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static void search() throws IOException, SQLException {
		
		long startTime = 0;
		long endTime = 0;
		
		File rfile = new File(path + rFileName);
		FileReader fr;
		BufferedReader br;
		
		File wfile = new File(path + wFileName);
		FileWriter fw;
		BufferedWriter bw;
		
		
		fr = new FileReader(rfile);
		br = new BufferedReader(fr);

		fw = new FileWriter(wfile);
		bw = new BufferedWriter(fw);
		
		String tempSql;
		while ((tempSql = br.readLine()) != null) {
			tempSql = tempSql.replaceFirst("tag2", "TAG_PPP");
//			tempSql="select eventId from TAG_PPP  where runNo= '-8126' and totalCharged ='5'";
//			System.out.println(tempSql);
			startTime = System.currentTimeMillis();
			rset = stmt.executeQuery(tempSql);
			int count = 0;
			while (rset.next()) {
				count++;
			}
			endTime = System.currentTimeMillis();
			bw.write(tempSql+","+(endTime-startTime)+","+count+"\n");
		}
		br.close();
		bw.close();
		stmt.close();
		con.close();
	}
}

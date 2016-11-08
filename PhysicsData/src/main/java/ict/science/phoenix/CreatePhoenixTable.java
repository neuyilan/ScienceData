package ict.science.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CreatePhoenixTable {
	
	public static void main(String[] args) throws SQLException{
		Statement stmt = null;
		ResultSet rset = null;
		Connection con = DriverManager.getConnection("jdbc:phoenix:172.22.1.56:2181");
		stmt=con.createStatement();
		stmt.executeUpdate("create table test_2 (mykey integer not null primary key,mycolumn varchar)");
		con.commit();
		
		PreparedStatement paStat = con.prepareStatement("select * from test_2");
		rset  = paStat.executeQuery();
		
		while(rset.next()){
			System.out.println(rset.getInt("mykey")+","+rset.getString("mycolumn"));
		}
		paStat.close();
		con.close();
	}
}

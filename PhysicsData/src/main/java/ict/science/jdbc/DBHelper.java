package ict.science.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
  
public class DBHelper {  
    public static final String url = "jdbc:mysql://172.22.1.58:3306/rhp";  
    public static final String name = "com.mysql.jdbc.Driver";  
    public static final String user = "root";  
    public static final String password = "data@ict";  
  
    public Connection conn = null;  
   
    
    public DBHelper() { 
    	
    }
  
    public DBHelper(String sql) {  
        try {  
            Class.forName(name);//指定连接类型  
            conn = DriverManager.getConnection(url, user, password);//获取连接  
//            pst = conn.prepareStatement(sql);//准备执行语句  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
    
    public Connection getConn(){  
        try {  
            Class.forName(name);//指定连接类型  
            conn = DriverManager.getConnection(url, user, password);//获取连接  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        return conn;
    }  
    
  
    public void close() {  
        try {  
            this.conn.close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
}  

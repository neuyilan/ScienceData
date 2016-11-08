package ict.science.dealdata.nomr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ExtractRowKey {
	private static String SEPARATOR=",";
	
	//the software number attribute
	private static String SOFTNO="softNo";
	
	//the software number attribute value, this is only a fake value, for test
	private static String SOFTNOVALUE="0";
	
	private static String[] title = {"entry","runNo","eventId","totalCharged","totalNeutral","totalTrks"};
	
	private Map<String,String> result= new HashMap<String,String>();
	
	
	/**
	 * write the result map to file.csv
	 * @param fileName	the file to write
	 */
	public void writeFile(String path,String fileName){
		BufferedWriter bw = null;
		try {
			File file = new File(path+fileName);
			bw = new BufferedWriter(new FileWriter(file,true));
			for(Entry<String,String> entry: result.entrySet()){
				bw.write(entry.getKey()+","+entry.getValue()+"\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(bw!=null){
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/**
	 * 
	 * @param fileName	the csv file name
	 * return the row key schema file, there is the map type
	 */
	public void readFile(String path,String fileName){
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(path+fileName));
			String lineStr = null;
			String item[] = null;
			String key =null;
			String value = null;
			while((lineStr=reader.readLine())!=null){
				item = lineStr.split(SEPARATOR);
				for(int i=3; i<item.length; i++){
					key = SOFTNO+"#"+SOFTNOVALUE+"#"+"runNo"+"#"+item[1]+"#"+title[i]+"#"+item[i];
					
					if(result.containsKey(key)){
						value = result.get(key)+"#"+item[2];
						
					}else{
						value = item[2];
					}
					result.put(key, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(reader!=null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String args[]){
		String path = "/home/qhl/scienceData/data/";
		String readFile = "Tag.csv";
		String writeFile = "result.csv";
		ExtractRowKey extractRowKey = new ExtractRowKey();
		extractRowKey.readFile(path, readFile);
		extractRowKey.writeFile(path, writeFile);
	}
	
}

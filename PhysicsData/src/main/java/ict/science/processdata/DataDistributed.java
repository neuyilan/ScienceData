package ict.science.processdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DataDistributed {

	public static void main(String args[]) {
		String path = "/home/qhl/physicsData/data/";
		String rFileName = "in";
		String wFileName = "out.csv";
		attrValueDis(path+rFileName,path+wFileName);

	}

	/**
	 * get the attributed value distributed.
	 * 
	 * @return
	 */
	public static Map<String, Integer> attrValueDis(String rfileName,
			String wfileName) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		File file = new File(rfileName);
		BufferedReader br = null;
		try {
			FileReader fr = new FileReader(file);
			br = new BufferedReader(fr);
			String tempStr = null;
			String arr[] = null;
			int count = 0;
			while ((tempStr = br.readLine()) != null) {
				arr = tempStr.split("\t");
				count = arr[1].split("#").length;
				result.put(arr[0], count);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		List<Map.Entry<String,Integer>> listData =  new ArrayList<Map.Entry<String,Integer>>(result.entrySet());
		DataDistributed.ByValueComparator bv = new ByValueComparator();
		Collections.sort(listData,bv);
		
		/******************* calculate the num ***********************************/
		Map<Integer,Integer> tempMap = new HashMap<Integer,Integer>();
		for (Entry<String, Integer> entry : listData) {
			Integer key  = entry.getValue();
			if(tempMap.containsKey(key)){
				tempMap.put(key, tempMap.get(key)+1);
			}else{
				tempMap.put(key, 1);
			}
		}
		List<Map.Entry<Integer,Integer>> tempListData =  new ArrayList<Map.Entry<Integer,Integer>>(tempMap.entrySet());
		DataDistributed.ByKeyIntComparator tempBv = new ByKeyIntComparator();
		Collections.sort(tempListData,tempBv);
		
		File wfile = new File(wfileName);
		FileWriter fw;
		BufferedWriter bw;
		try {
			fw = new FileWriter(wfile);
			bw = new BufferedWriter(fw);
			for (Entry<Integer, Integer> entry : tempListData) {
				
				bw.write(entry.getKey() + "," + entry.getValue() + "\n");
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	/******************* calculate the num ***********************************/
		
		
		
		
		
//		
//		File wfile = new File(wfileName);
//		FileWriter fr;
//		BufferedWriter bw;
//		try {
////			fr = new FileWriter(wfile, true);
//			fr = new FileWriter(wfile);
//			bw = new BufferedWriter(fr);
//			for (Entry<String, Integer> entry : listData) {
//				bw.write(entry.getKey() + "," + entry.getValue() + "\n");
//			}
//			bw.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		
		return result;
	}
	
	
	
	
	static class ByKeyComparator implements Comparator<Map.Entry<String, Integer>>{
		public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
			return o1.getKey().toString().compareTo(o2.getKey().toString());
		}
	}
	
	
	static class ByValueComparator implements  Comparator<Map.Entry<String, Integer>>{
		public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
			return o1.getValue()-o2.getValue();
		}
	}
	
	static class ByKeyIntComparator implements Comparator<Map.Entry<Integer, Integer>>{
		public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
			return o1.getKey()-o2.getKey();
		}
	}

}

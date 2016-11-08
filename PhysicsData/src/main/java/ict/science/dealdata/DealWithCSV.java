package ict.science.dealdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DealWithCSV {

	static String path = "/home/qhl/physicsData/data/";
	static String rFileName = "count.csv";
	static String wFileName = "query_count.csv";

	public static void main(String args[]) {
		try {
			statisCount();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void statisCount() throws IOException {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();

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

		String tempSql = "";
		while ((tempSql = br.readLine()) != null) {
			Integer key = Integer.parseInt(tempSql);
			if (map.get(key) == null) {
				map.put(key, 1);
			} else {
				map.put(key, 1 + map.get(key));
			}
		}

		for (Entry<Integer, Integer> entry : map.entrySet()) {
			bw.write(entry.getKey() + "," + entry.getValue() + "\n");
		}

		br.close();
		bw.close();
	}

	public static void dealWithCSV() throws IOException {
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

		String tempSql = "";
		long count = 1;
		while ((tempSql = br.readLine()) != null) {
			bw.write(count + "," + tempSql + "\n");
			count++;
		}

		br.close();
		bw.close();

	}

}

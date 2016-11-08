package ict.science.jdbc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateRandSql {
	private static int runNo[] = { 8093, 8291 };
	private static int totalCharged[] = { 0, 15 };
	private static int totalNeutral[] = { 0, 30 };
	private static int totalTrks[] = { 0, 50 };
//	private static int count = 1000;

	static String path = "/home/qhl/physicsData/data/";
	static String wFileName = "query.sql";

	public static void main(String args[]) {
		generateRandSql();
	}

	public static void generateRandSql() {
		Random random = new Random();
		String sqlCharged;
		int randRunNo = 0;
		int randCharged = 0;
		int randNeutral = 0;
		int randTrks = 0;

		File wfile = new File(path+wFileName);
		FileWriter fr;
		BufferedWriter bw=null;
		
		try {
			fr = new FileWriter(wfile);
			bw = new BufferedWriter(fr);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		for (int i = 0; i < 333; i++) {
			randCharged = random.nextInt(totalCharged[1]);
			randRunNo = random.nextInt(runNo[1] - runNo[0]) + runNo[0];
			sqlCharged = "select eventId from tag2  where runNo= '-"
					+ randRunNo + "' and totalCharged ='" + randCharged + "'";
			
			try {
				bw.write(sqlCharged+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(sqlCharged);
		}
		
		
		for (int i = 0; i < 333; i++) {
			randNeutral = random.nextInt(totalNeutral[1]);
			randRunNo = random.nextInt(runNo[1] - runNo[0]) + runNo[0];
			sqlCharged = "select eventId from tag2  where runNo= '-"
					+ randRunNo + "' and totalNeutral ='" + randNeutral + "'";
			try {
				bw.write(sqlCharged+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(sqlCharged);
		}
		
		
		
		for (int i = 0; i < 334; i++) {
			randTrks = random.nextInt(totalTrks[1]);
			randRunNo = random.nextInt(runNo[1] - runNo[0]) + runNo[0];
			sqlCharged = "select eventId from tag2  where runNo= '-"
					+ randRunNo + "' and totalTrks ='" + randTrks + "'";
			try {
				bw.write(sqlCharged+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(sqlCharged);
		}
		
		try{
			if(bw!=null){
				bw.close();
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

}

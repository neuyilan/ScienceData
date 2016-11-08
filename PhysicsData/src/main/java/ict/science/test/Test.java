package ict.science.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class Test {
	public static void main(String[] args){
		File file  = new File("/home/qhl/scienceData/data/part5");
		try {
			BufferedReader br  = new BufferedReader(new FileReader(file));
			String temp=null;
			while((temp=br.readLine())!=null){
				temp = br.readLine();
				String arr[] = temp.split(" ");
				System.out.println("***"+arr.length);
				System.out.println(arr[0]);
				System.out.println(arr[1]);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
}

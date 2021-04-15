package main.java.hadoop;

import java.util.StringTokenizer;

public class LineSplitter {

	public String subnet = null;
	public String ip = null;
	
	public LineSplitter(String line) {
		StringTokenizer itr = new StringTokenizer(line);
		ip = itr.nextToken();
	}

	
	public void parse() {
		String[] subs = ip.split(".");
		int l = subs.length;
		String last = subs[l - 1];
		
		try {
			Integer.parseInt(last);
			subnet = null;
		} catch(NumberFormatException e) {
			String second_to_last = subs[l - 2];
			subnet = second_to_last + "." + last;
		}
	}
	
}

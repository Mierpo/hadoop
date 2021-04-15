package main.java.hadoop;

import java.util.StringTokenizer;

public class LineSplitter {

	public String subnet = null;
	public String ip = null;
	
	//value: [199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245]
	// ip: [199.72.81.55]Index -1 out of bounds for length 0
	
	public LineSplitter(String line) {
		StringTokenizer itr = new StringTokenizer(line);
		ip = itr.nextToken();
	}

	
	public void parse() {
		String[] subs = ip.split("\\.");
		int l = subs.length;
		if(l < 2) {
			subnet = ip; //Just put the entire IP there
		}
		
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

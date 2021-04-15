package main.java.hadoop;

public class LineSplitter {

	public String subnet = null;
	
	public LineSplitter(String line) {
		int f = line.indexOf(" ");
		String ip = line.substring(0, f);
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

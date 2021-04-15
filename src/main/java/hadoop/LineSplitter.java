package main.java.hadoop;

public class LineSplitter {

	public int bytes = 0;
	public int last_space = -1;
	public String substring = "";
	
	public LineSplitter(String line) {
		last_space = line.lastIndexOf(" ");
		substring = line.substring(last_space + 1);
	}
	
	public void parse() {
		try {
			bytes = Integer.parseInt(substring);
		} catch(NumberFormatException e) {
			bytes = 0;
		}
	}
	
}

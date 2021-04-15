package main.java.hadoop;

public class LineSplitter {

	public int bytes = 0;
	
	public LineSplitter(String line) {
		int last_space = line.lastIndexOf(" ");
		bytes = Integer.parseInt(line.substring(last_space + 1));
	}
	
}

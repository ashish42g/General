package replicateFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Replica {

	static Random randomGeneratorNames = new Random();
	static Random randomGeneratorSubject = new Random();
	static Random randomGeneratorStatus = new Random();
	static ArrayList<String> names = new ArrayList<String>();
	static ArrayList<String> subjects = new ArrayList<String>();
	static ArrayList<String> status = new ArrayList<String>();
	static int index;

	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n");
			System.exit(-1);
		}

		String in = args[0];
		String out = args[1];

		initializeRandomData();
		convert(in, out);

	}

	private static void convert(String inPath, String outPath)
			throws IOException {

		BufferedReader reader = null;
		BufferedWriter writer = null;
		String line;

		try {
			reader = new BufferedReader(new FileReader(inPath));
			writer = new BufferedWriter(new FileWriter(outPath));

			while ((line = reader.readLine()) != null) {

				String[] fields = line.split(",");
				String newLine = "";

				for (int i = 0; i < 100; i++) {
					for (int j = 0; j < fields.length; j++) {

						if (j == fields.length - 1) {
							newLine += fields[j];
						} else {

							if (j == 0) {
								int index = randomGeneratorNames.nextInt(names.size());
								newLine += "\"" + names.get(index) + "\"" + ",";
							} else if (j == 1) {
								int index = randomGeneratorSubject.nextInt(subjects.size());
								newLine += "\"" + subjects.get(index) + "\""+ ",";
							} else if (j == 4) {
								index = randomGeneratorStatus.nextInt(status.size());
								newLine += "\"" + status.get(index).toString() + "\"" +",";
							} else if (j == 5) {
								if (status.get(index).toString().equalsIgnoreCase("1")) {
									newLine +=  "\"" + "0" + "\"" + ",";
								} else if (status.get(index).toString().equalsIgnoreCase("0")) {
									newLine += "\"" + "1" + "\"" + ",";
								}
							} else {
								newLine += fields[j] + ",";
							}
						}
					}
					newLine += '\n';
					writer.write(newLine);
				}
			}

		} catch (Exception exc) {
			System.out.print(exc.getMessage());
		} finally {
			reader.close();
			writer.close();
			System.out.println("completed sucessfully");
		}
	}

	private static void initializeRandomData() {
		names.add("Ravi Prakash");
		names.add("Rita Verma");
		names.add("Souhash Gosh");
		names.add("Ravish Metha");
		names.add("Ravinder Roy");
		names.add("Rahul Rathi");
		names.add("Ashish Garg");
		names.add("Ashish Gupta");
		names.add("Sanjna Verma");
		names.add("Gaurav Solanki");
		names.add("Ashna Singh");
		names.add("Hapreet Kaur");
		names.add("Bindra Bandik");
		names.add("Chanderpal Singh");
		names.add("Deepak Dhillon");
		names.add("Ema Dhillon");
		names.add("keriti Dhillon");
		names.add("Suresh Singh");
		names.add("Radha Singh");
		names.add("Karan Roy");
		names.add("Yogesh Singh");
		names.add("Sameer Ashwani");
		names.add("kiran Kaur");

		subjects.add("Chat");
		subjects.add("Email");
		subjects.add("Call");
		subjects.add("Scheduled");
		subjects.add("Callback");
		subjects.add("OKM");
		subjects.add("WIFI");

		status.add("1");
		status.add("0");
	}
}
package com.stackroute.datamunger.reader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;

	// Parameterized constructor to initialize filename

	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file. Note: Return type of the method will be
	 * Header
	 */

	@Override
	public Header getHeader() throws IOException {
		String[] headeData = null;
		BufferedReader bufferedReader = null;
		try {
			// read the first row
			bufferedReader = new BufferedReader(new FileReader(getFileName()));
			String headeDataResult = bufferedReader.readLine();
			// populate the header object with the String array containing the header names
			headeData = headeDataResult.split(",");
		} finally {
			if (bufferedReader != null)
				bufferedReader.close();
		}
		return new Header(headeData);
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */

	@Override
	public void getDataRow() {
	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. Note: Return Type of the method will be
	 * DataTypeDefinitions
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinitions = null;
		BufferedReader bufferedReader = null;
		try {
			try {
				// read line
				bufferedReader = new BufferedReader(new FileReader(getFileName()));
			} catch (FileNotFoundException e) {
				bufferedReader = new BufferedReader(new FileReader("data/ipl.csv"));
			}
			bufferedReader.readLine();// header 
			String dataLineStr = bufferedReader.readLine();// data
			String[] columns = dataLineStr.split(",", 18);
			String[] dataTypeArray = new String[columns.length];
			int count = 0;
			for (String s : columns) {
				if (s.matches("[0-9]+")) {
					dataTypeArray[count] = "java.lang.Integer";
					count++;
				} else {
					dataTypeArray[count] = "java.lang.String";
					count++;
				}
			}
			dataTypeDefinitions = new DataTypeDefinitions(dataTypeArray);
		} finally {
			if (bufferedReader != null)
				bufferedReader.close();
		}
		return dataTypeDefinitions;
	}
}
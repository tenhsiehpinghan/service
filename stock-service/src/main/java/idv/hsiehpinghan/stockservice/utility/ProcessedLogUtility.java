package idv.hsiehpinghan.stockservice.utility;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;

public class ProcessedLogUtility {
	private static final String[] EXTENSIONS = { "log" };
	private static final String FILE_NAME = "processed.log";

	public static void main(String[] args) throws Exception {
		String dirPath = "/home/hsiehpinghan/Desktop/stock_test/download/mops/monthly_operating_income";
		deleteProcessedLogFile(new File(dirPath));
	}

	public static void deleteProcessedLogFile(File dir) throws IOException {
		Collection<File> files = FileUtils.listFiles(dir, EXTENSIONS, true);
		for (File file : files) {
			if (file.getName().equals(FILE_NAME)) {
				file.delete();
			}
		}
	}

}

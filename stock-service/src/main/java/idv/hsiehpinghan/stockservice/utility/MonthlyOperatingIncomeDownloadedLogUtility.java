package idv.hsiehpinghan.stockservice.utility;

import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.stockservice.operator.MonthlyOperatingIncomeDownloader;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;

public class MonthlyOperatingIncomeDownloadedLogUtility {
	private static final String[] EXTENSIONS = { "csv" };

	public static void main(String[] args) throws Exception {
		String dirPath = "/home/hsiehpinghan/Desktop/stock_test/download/mops/monthly_operating_income";
		refresh(new File(dirPath));
	}

	public static void refresh(File dir) throws IOException {
		Collection<File> files = FileUtils.listFiles(dir, EXTENSIONS, false);
		if (files.size() <= 0) {
			for (File subFile : FileUtility.listDirectories(dir)) {
				refresh(subFile);
			}
		} else {
			File downloadLog = recreateDownloadedLog(dir);
//			String oldFileName = "";
			for (File file : new TreeSet<File>(files)) {
//				if (oldFileName.startsWith(getTest(file)) == false) {
					String downloadInfo = getDownloadInfo(file);
					FileUtils.write(downloadLog, downloadInfo, Charsets.UTF_8,
							true);
//				}
//				oldFileName = file.getName();
			}
		}
	}

//	private static String getTest(File file) {
//		String[] sArr =file.getName().split("\\.");
//		return sArr[0].split("_")[0];
//	}
	private static String getDownloadInfo(File file) {
		return file.getName().split("\\.")[0] + System.lineSeparator();
	}

	private static File recreateDownloadedLog(File dir) throws IOException {
		File f = new File(dir,
				MonthlyOperatingIncomeDownloader.DOWNLOADED_LOG_FILE_NAME);
		FileUtils.deleteQuietly(f);
		FileUtils.touch(f);
		return f;
	}

}

package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.datetimeutility.utility.DateUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.stockdao.entity.StockClosingCondition;
import idv.hsiehpinghan.stockdao.entity.StockClosingCondition.ClosingConditionFamily;
import idv.hsiehpinghan.stockdao.repository.StockClosingConditionRepository;
import idv.hsiehpinghan.stockservice.manager.IStockClosingConditionManager;
import idv.hsiehpinghan.stockservice.operator.StockClosingConditionOfGretaiDownloader;
import idv.hsiehpinghan.stockservice.operator.StockClosingConditionOfTwseDownloader;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.common.base.Charsets;

@Service
public class StockClosingConditionHbaseManager implements
		IStockClosingConditionManager, InitializingBean {
	private final String[] EXTENSIONS = { "csv" };
	private final String BIG5 = "big5";
	private final String SPACE_STRING = StringUtility.SPACE_STRING;
	private final String COMMA_STRING = StringUtility.COMMA_STRING;
	private final String EMPTY_STRING = StringUtility.EMPTY_STRING;
	private final String DOUBLE_UOTATION_STRING = StringUtility.DOUBLE_UOTATION_STRING;
	private final String YYYYMMDD = "yyyyMMdd";
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDirOfTwse;
	private File processedLogOfTwse;
	private File downloadDirOfGretai;
	private File processedLogOfGretai;

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private StockServiceProperty stockServiceProperty;
	// @Autowired
	// private StockClosingConditionOfTwseDownloader downloaderOfTwse;
	// @Autowired
	// private StockClosingConditionOfGretaiDownloader downloaderOfGretai;
	@Autowired
	private StockClosingConditionRepository conditionRepo;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDirOfTwse = stockServiceProperty
				.getStockClosingConditionDownloadDirOfTwse();
		downloadDirOfGretai = stockServiceProperty
				.getStockClosingConditionDownloadDirOfGretai();
		generateProcessedLogFiles();
	}

	@Override
	public synchronized boolean updateStockClosingCondition() {
		boolean result = true;
		try {
			updateStockClosingConditionOfTwse();
		} catch (Exception e) {
			logger.error("Update stock closing condition of twse fail !!!");
			e.printStackTrace();
			result = false;
		}
		try {
			updateStockClosingConditionOfGretai();
		} catch (Exception e) {
			logger.error("Update stock closing condition of gretai fail !!!");
			e.printStackTrace();
			result = false;
		}
		return result;
	}

	@Override
	public TreeSet<StockClosingCondition> getAll(String stockCode) {
		return conditionRepo.fuzzyScan(stockCode, null);
	}

//	@Scheduled(cron="0 0 22 * * *")
	public void scheduledUpdateStockClosingCondition() {
		updateStockClosingCondition();
	}
	
	boolean updateStockClosingConditionOfTwse() throws NoSuchFieldException,
			SecurityException, IllegalArgumentException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException, InstantiationException, ParseException,
			IOException {
		File dir = downloadStockClosingConditionOfTwse();
		if (dir == null) {
			return false;
		}
		int processFilesAmt = saveStockClosingConditionOfTwseToHBase(dir);
		logger.info("Saved " + processFilesAmt + " files to "
				+ conditionRepo.getTargetTableName() + ".");
		return true;
	}

	boolean updateStockClosingConditionOfGretai() throws NoSuchFieldException,
			SecurityException, IllegalArgumentException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException, InstantiationException, ParseException,
			IOException {
		File dir = downloadStockClosingConditionOfGretai();
		if (dir == null) {
			return false;
		}
		int processFilesAmt = saveStockClosingConditionOfGretaiToHBase(dir);
		logger.info("Saved " + processFilesAmt + " files to "
				+ conditionRepo.getTargetTableName() + ".");
		return true;
	}

	int saveStockClosingConditionOfTwseToHBase(File dir) throws ParseException,
			IOException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException,
			NoSuchMethodException, InvocationTargetException,
			InstantiationException {
		int count = 0;
		Date ver = new Date();
		Set<String> processedSet = FileUtility
				.readLinesAsHashSet(processedLogOfTwse);
		// ex. A11220130104ALLBUT0999.csv
		for (File file : FileUtils.listFiles(dir, EXTENSIONS, true)) {
			if (isProcessed(processedSet, file)) {
				continue;
			}
			Date date = DateUtils.parseDate(file.getName().substring(4, 12),
					YYYYMMDD);
			List<String> lines = FileUtils.readLines(file, BIG5);
			if (hasDataOfTwse(file, date, lines) == false) {
				continue;
			}
			int startRow = getStartRowOfTwse(file, lines);
			int size = lines.size();
			List<StockClosingCondition> entities = new ArrayList<StockClosingCondition>(
					size - startRow);
			for (int i = startRow; i < size; ++i) {
				String line = lines.get(i).replace(DOUBLE_UOTATION_STRING,
						EMPTY_STRING);
				String[] strArr = line.split(COMMA_STRING);
				if (strArr.length <= 1) {
					break;
				}
				String stockCode = getString(strArr[0]);
				if (conditionRepo.exists(stockCode, date)) {
					continue;
				}
				BigDecimal openingPrice = getBigDecimalOfTwse(strArr[5]);
				BigDecimal closingPrice = getBigDecimalOfTwse(strArr[8]);
				BigDecimal change = getBigDecimalOfTwse(strArr[9], strArr[10]);
				BigDecimal highestPrice = getBigDecimalOfTwse(strArr[6]);
				BigDecimal lowestPrice = getBigDecimalOfTwse(strArr[7]);
				BigDecimal finalPurchasePrice = getBigDecimalOfTwse(strArr[11]);
				BigDecimal finalSellingPrice = getBigDecimalOfTwse(strArr[13]);
				BigInteger stockAmount = getBigIntegerOfTwse(strArr[2]);
				BigInteger moneyAmount = getBigIntegerOfTwse(strArr[4]);
				BigInteger transactionAmount = getBigIntegerOfTwse(strArr[3]);
				StockClosingCondition entity = generateEntity(stockCode, date,
						ver, change, closingPrice, finalPurchasePrice,
						finalSellingPrice, highestPrice, lowestPrice,
						moneyAmount, openingPrice, stockAmount,
						transactionAmount);
				entities.add(entity);
			}
			conditionRepo.put(entities);
			writeToProcessedFileOfTwse(file);
			logger.info(file.getName() + " saved to "
					+ conditionRepo.getTargetTableName() + ".");
			++count;
		}
		return count;
	}

	int saveStockClosingConditionOfGretaiToHBase(File dir)
			throws ParseException, IOException, NoSuchFieldException,
			SecurityException, IllegalArgumentException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException, InstantiationException {
		int count = 0;
		Date ver = new Date();
		Set<String> processedSet = FileUtility
				.readLinesAsHashSet(processedLogOfGretai);
		// ex. SQUOTE_EW_1020107.csv
		for (File file : FileUtils.listFiles(dir, EXTENSIONS, true)) {
			if (isProcessed(processedSet, file)) {
				continue;
			}
			Date date = DateUtility.parseRocDate(
					file.getName().substring(10, 17), YYYYMMDD);
			List<String> lines = FileUtils.readLines(file, BIG5);
			if (hasDataOfGretai(file, date, lines) == false) {
				continue;
			}
			int startRow = getStartRowOfGretai(file, lines);
			int size = lines.size();
			List<StockClosingCondition> entities = new ArrayList<StockClosingCondition>(
					size - startRow);
			for (int i = startRow; i < size; ++i) {
				String[] strArr = lines.get(i).split("\",\"");
				if (strArr.length <= 1) {
					break;
				}
				String stockCode = getString(strArr[0].replace(
						DOUBLE_UOTATION_STRING, EMPTY_STRING));
				if (conditionRepo.exists(stockCode, date)) {
					continue;
				}
				BigDecimal openingPrice = getBigDecimalOfGretai(strArr[4]);
				BigDecimal closingPrice = getBigDecimalOfGretai(strArr[2]);
				BigDecimal change = getBigDecimalOfGretai(strArr[3].replace(
						SPACE_STRING, EMPTY_STRING));
				BigDecimal highestPrice = getBigDecimalOfGretai(strArr[5]);
				BigDecimal lowestPrice = getBigDecimalOfGretai(strArr[6]);
				BigDecimal finalPurchasePrice = getBigDecimalOfGretai(strArr[10]);
				BigDecimal finalSellingPrice = getBigDecimalOfGretai(strArr[11]);
				BigInteger stockAmount = getBigIntegerOfGretai(strArr[7]);
				BigInteger moneyAmount = getBigIntegerOfGretai(strArr[8]);
				BigInteger transactionAmount = getBigIntegerOfGretai(strArr[9]);
				StockClosingCondition entity = generateEntity(stockCode, date,
						ver, change, closingPrice, finalPurchasePrice,
						finalSellingPrice, highestPrice, lowestPrice,
						moneyAmount, openingPrice, stockAmount,
						transactionAmount);
				entities.add(entity);
			}
			conditionRepo.put(entities);
			writeToProcessedFileOfGretai(file);
			logger.info(file.getName() + " saved to "
					+ conditionRepo.getTargetTableName() + ".");
			++count;
		}
		return count;
	}

	private boolean hasDataOfTwse(File file, Date date, List<String> lines)
			throws IOException {
		String targetDateStr = DateFormatUtils.format(date, "yyyy年MM月dd日");
		for (String line : lines) {
			String trimmedLine = line.trim();
			if (trimmedLine.startsWith(targetDateStr)) {
				if (trimmedLine.endsWith("查無資料")) {
					return false;
				}
				return true;
			}
		}
		throw new RuntimeException("File(" + file.getAbsolutePath()
				+ ") has wrong date !!!");
	}

	private boolean hasDataOfGretai(File file, Date date, List<String> lines)
			throws IOException {
		if (lines.size() == 0) {
			return false;
		}
		String targetDateStr = DateUtility.getRocDateString(date, "yyyy/MM/dd");
		for (String line : lines) {
			String trimmedLine = line.trim();
			if (trimmedLine.endsWith(targetDateStr)) {
				return true;
			}
		}
		throw new RuntimeException("File(" + file.getAbsolutePath()
				+ ") has wrong date !!!");
	}

	private int getStartRowOfTwse(File file, List<String> lines) {
		String targetStr = "\"證券代號\",\"證券名稱\",\"成交股數\",\"成交筆數\",\"成交金額\",\"開盤價\",\"最高價\",\"最低價\",\"收盤價\",\"漲跌(+/-)\",\"漲跌價差\",\"最後揭示買價\",\"最後揭示買量\",\"最後揭示賣價\",\"最後揭示賣量\",\"本益比\"";
		for (int i = 0, size = lines.size(); i < size; ++i) {
			if (targetStr.equals(lines.get(i))) {
				return i + 1;
			}
		}
		throw new RuntimeException("File(" + file.getAbsolutePath()
				+ ") cannot find line(" + targetStr + ") !!!");
	}

	private int getStartRowOfGretai(File file, List<String> lines) {
		String targetStr = "代號,名稱,收盤 ,漲跌,開盤 ,最高 ,最低,成交股數  , 成交金額(元), 成交筆數 ,最後買價,最後賣價,發行股數 ,次日漲停價 ,次日跌停價";
		for (int i = 0, size = lines.size(); i < size; ++i) {
			if (targetStr.equals(lines.get(i))) {
				return i + 1;
			}
		}
		throw new RuntimeException("File(" + file.getAbsolutePath()
				+ ") cannot find line(" + targetStr + ") !!!");
	}

	private BigDecimal getBigDecimalOfTwse(String sign, String val) {
		if ("X".equals(sign)) {
			return getBigDecimalOfTwse(val);
		} else {
			return getBigDecimalOfTwse(sign + val);
		}
	}

	private BigDecimal getBigDecimalOfTwse(String str) {
		String trimmedStr = str.trim();
		if ("--".equals(trimmedStr)) {
			return null;
		}
		return new BigDecimal(trimmedStr);
	}

	private BigDecimal getBigDecimalOfGretai(String str) {
		String trimmedStr = str.trim();
		if (trimmedStr.startsWith("---")) {
			return null;
		} else if (trimmedStr.equals("除權息")) {
			return null;
		} else if (trimmedStr.equals("除權")) {
			return null;
		} else if (trimmedStr.equals("除息")) {
			return null;
		}
		return new BigDecimal(str);
	}

	private BigInteger getBigIntegerOfTwse(String str) {
		String trimmedStr = str.trim();
		if ("--".equals(trimmedStr)) {
			return null;
		}
		return new BigInteger(trimmedStr);
	}

	private BigInteger getBigIntegerOfGretai(String str) {
		return new BigInteger(str.replace(COMMA_STRING, EMPTY_STRING));
	}

	private String getString(String str) {
		return str.trim();
	}

	private boolean isProcessed(Set<String> processedSet, File file)
			throws IOException {
		String processedInfo = generateProcessedInfo(file);
		if (processedSet.contains(processedInfo)) {
			logger.info(processedInfo + " processed before.");
			return true;
		}
		return false;
	}

	private String generateProcessedInfo(File file) {
		return file.getName();
	}

	private void writeToProcessedFileOfTwse(File file) throws IOException {
		String infoLine = generateProcessedInfo(file) + System.lineSeparator();
		FileUtils.write(processedLogOfTwse, infoLine, Charsets.UTF_8, true);
	}

	private void writeToProcessedFileOfGretai(File file) throws IOException {
		String infoLine = generateProcessedInfo(file) + System.lineSeparator();
		FileUtils.write(processedLogOfGretai, infoLine, Charsets.UTF_8, true);
	}

	private StockClosingCondition generateEntity(String stockCode, Date date,
			Date ver, BigDecimal change, BigDecimal closingPrice,
			BigDecimal finalPurchasePrice, BigDecimal finalSellingPrice,
			BigDecimal highestPrice, BigDecimal lowestPrice,
			BigInteger moneyAmount, BigDecimal openingPrice,
			BigInteger stockAmount, BigInteger transactionAmount) {
		StockClosingCondition entity = new StockClosingCondition();
		entity.new RowKey(stockCode, date, entity);
		generateClosingConditionFamilyContent(entity, ver, change,
				closingPrice, finalPurchasePrice, finalSellingPrice,
				highestPrice, lowestPrice, moneyAmount, openingPrice,
				stockAmount, transactionAmount);
		return entity;
	}

	private void generateClosingConditionFamilyContent(
			StockClosingCondition entity, Date ver, BigDecimal change,
			BigDecimal closingPrice, BigDecimal finalPurchasePrice,
			BigDecimal finalSellingPrice, BigDecimal highestPrice,
			BigDecimal lowestPrice, BigInteger moneyAmount,
			BigDecimal openingPrice, BigInteger stockAmount,
			BigInteger transactionAmount) {
		ClosingConditionFamily fam = entity.getClosingConditionFamily();
		fam.setChange(ver, change);
		fam.setClosingPrice(ver, closingPrice);
		fam.setFinalPurchasePrice(ver, finalPurchasePrice);
		fam.setFinalSellingPrice(ver, finalSellingPrice);
		fam.setHighestPrice(ver, highestPrice);
		fam.setLowestPrice(ver, lowestPrice);
		fam.setMoneyAmount(ver, moneyAmount);
		fam.setOpeningPrice(ver, openingPrice);
		fam.setStockAmount(ver, stockAmount);
		fam.setTransactionAmount(ver, transactionAmount);
	}

	private File downloadStockClosingConditionOfTwse() {
		StockClosingConditionOfTwseDownloader downloaderOfTwse = applicationContext
				.getBean(StockClosingConditionOfTwseDownloader.class);
		try {
			File dir = downloaderOfTwse.downloadStockClosingCondition();
			logger.info(dir.getAbsolutePath() + " download finish.");
			return dir;
		} catch (Exception e) {
			logger.error("Download stock closing condition fail !!!");
			return null;
		}
	}

	private File downloadStockClosingConditionOfGretai() {
		StockClosingConditionOfGretaiDownloader downloaderOfGretai = applicationContext
				.getBean(StockClosingConditionOfGretaiDownloader.class);
		try {
			File dir = downloaderOfGretai.downloadStockClosingCondition();
			logger.info(dir.getAbsolutePath() + " download finish.");
			return dir;
		} catch (Exception e) {
			logger.error("Download stock closing condition fail !!!");
			return null;
		}
	}

	private void generateProcessedLogFiles() throws IOException {
		generateProcessedLogFileOfTwse();
		generateProcessedLogFileOfGretai();
	}

	private void generateProcessedLogFileOfTwse() throws IOException {
		if (processedLogOfTwse == null) {
			processedLogOfTwse = new File(downloadDirOfTwse, "processed.log");
			if (processedLogOfTwse.exists() == false) {
				FileUtils.touch(processedLogOfTwse);
			}
		}
	}

	private void generateProcessedLogFileOfGretai() throws IOException {
		if (processedLogOfGretai == null) {
			processedLogOfGretai = new File(downloadDirOfGretai,
					"processed.log");
			if (processedLogOfGretai.exists() == false) {
				FileUtils.touch(processedLogOfGretai);
			}
		}
	}
}

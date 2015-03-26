package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.datatypeutility.utility.BigDecimalUtility;
import idv.hsiehpinghan.datatypeutility.utility.CharsetUtility;
import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.stockdao.entity.MonthlyOperatingIncome;
import idv.hsiehpinghan.stockdao.entity.MonthlyOperatingIncome.IncomeFamily;
import idv.hsiehpinghan.stockdao.enumeration.CurrencyType;
import idv.hsiehpinghan.stockdao.repository.MonthlyOperatingIncomeRepository;
import idv.hsiehpinghan.stockservice.manager.IMonthlyOperatingIncomeHbaseManager;
import idv.hsiehpinghan.stockservice.operator.MonthlyOperatingIncomeDownloader;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class MonthlyOperatingIncomeHbaseManager implements
		IMonthlyOperatingIncomeHbaseManager {
	public static final String PROCESSED_LOG_FILE_NAME = "processed.log";
	private final String COMMA_STRING = StringUtility.COMMA_STRING;
	private final String EMPTY_STRING = StringUtility.EMPTY_STRING;
	private final String[] EXTENSIONS = { "csv" };
	private final Charset UTF8 = CharsetUtility.UTF_8;
	private Logger logger = Logger.getLogger(this.getClass().getName());
	// private File downloadDir;
	private File processedLog;
	private Set<String> processedSet;

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private StockServiceProperty stockServiceProperty;
	// @Autowired
	// private MonthlyOperatingIncomeDownloader downloader;
	@Autowired
	private MonthlyOperatingIncomeRepository incomeRepo;

	// @Override
	// public void afterPropertiesSet() throws Exception {
	// downloadDir = stockServiceProperty
	// .getMonthlyOperatingIncomeDownloadDir();
	// // generateProcessedLog();
	// }

	@Override
	public synchronized boolean updateMonthlyOperatingIncome() {
		File dir = downloadMonthlyOperatingIncome();
		if (dir == null) {
			return false;
		}
		Date ver = new Date();
		try {
			int processFilesAmt = saveMonthlyOperatingIncomeToHBase(ver, dir);
			logger.info("Saved " + processFilesAmt + " files to "
					+ incomeRepo.getTargetTableName() + ".");
		} catch (Exception e) {
			logger.error("Update monthly operating income fail !!!");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public TreeSet<MonthlyOperatingIncome> getAll(String stockCode,
			boolean isFunctionalCurrency) {
		return incomeRepo.fuzzyScan(stockCode, isFunctionalCurrency, null,
				null, null);
	}

	private boolean isFunctionalCurrency(String str) {
		return str.startsWith("功能性貨幣");
	}

	private CurrencyType getCurrencyType(String str) {
		switch (str) {
		case EMPTY_STRING:
		case "新台幣":
		case "功能性貨幣(台幣)":
		case "功能性貨幣(新)":
		case "功能性貨幣(新台幣)":
		case "功能性貨幣(NTD)":
		case "功能性貨幣(TWD)":
			return CurrencyType.TWD;
		case "功能性貨幣(美元)":
		case "功能性貨幣(美金)":
		case "功能性貨幣(USD)":
		case "功能性貨幣(USD仟元)":
		case "功能性貨幣(usd)":
		case "功能性貨幣(USD 仟元)":
		case "功能性貨幣(美金仟元)":
			return CurrencyType.USD;
		case "功能性貨幣(人民幣)":
		case "功能性貨幣(RMB)":
		case "功能性貨幣(CNY)":
		case "功能性貨幣(RNB)":
		case "功能性貨幣(人民幣))":
			return CurrencyType.CNY;
		case "功能性貨幣(日圓)":
		case "功能性貨幣(日幣仟圓)":
		case "功能性貨幣(日幣千圓)":
			return CurrencyType.JPY;
		case "功能性貨幣(SGD)":
		case "功能性貨幣(新加坡幣)":
		case "功能性貨幣(新加坡)":
			return CurrencyType.SGD;
		case "功能性貨幣(港幣)":
		case "功能性貨幣(HKD)":
			return CurrencyType.HKD;
		case "功能性貨幣(泰銖)":
		case "功能性貨幣(泰珠)":
			return CurrencyType.THB;
		case "功能性貨幣(馬幣)":
			return CurrencyType.MYR;
		default:
			throw new RuntimeException("String(" + str
					+ ") not match any currencyType !!!");
		}
	}

	private BigDecimal getUnit(String str) {
		switch (str) {
		case "功能性貨幣(USD仟元)":
		case "功能性貨幣(USD 仟元)":
		case "功能性貨幣(美金仟元)":
		case "功能性貨幣(日幣仟圓)":
		case "功能性貨幣(日幣千圓)":
			return BigDecimalUtility.ONE_THOUSAND;
		default:
			return BigDecimal.ONE;
		}
	}

	private BigDecimal getMultiplied(BigDecimal value, BigDecimal unit) {
		return value.multiply(unit);
	}

	int saveMonthlyOperatingIncomeToHBase(Date ver, File dir)
			throws IOException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException,
			NoSuchMethodException, InvocationTargetException,
			InstantiationException {
		int count = 0;
		File[] dirs = FileUtility.listDirectories(dir);
		if (dirs.length > 0) {
			for (File d : dirs) {
				count += saveMonthlyOperatingIncomeToHBase(ver, d);
			}
		} else {
			readProcessedLogAndUpdateProcessedSet(dir);
			// ex. 1101_201301.csv
			for (File file : FileUtils.listFiles(dir, EXTENSIONS, false)) {
				if (isProcessed(file)) {
					continue;
				}
				List<String> lines = FileUtils.readLines(file, UTF8);
				int startRow = getStartRow(file, lines);
				String[] fnStrArr = file.getName().split("[_.]");
				String stockCode = fnStrArr[0];
				int year = Integer.valueOf(fnStrArr[1].substring(0, 4));
				int month = Integer.valueOf(fnStrArr[1].substring(4));
				int size = lines.size();
				List<MonthlyOperatingIncome> entities = new ArrayList<MonthlyOperatingIncome>(
						size - startRow);
				for (int i = startRow; i < size; ++i) {
					String line = lines.get(i);
					String[] strArr = line.split(COMMA_STRING, -1);
					boolean isFunctionalCurrency = isFunctionalCurrency(strArr[0]);
					CurrencyType currency = getCurrencyType(strArr[0]);
					BigDecimal unit = getUnit(strArr[0]);
					BigDecimal currentMonth = getMultiplied(new BigDecimal(
							strArr[1]), unit);
					BigDecimal currentMonthOfLastYear = getMultiplied(
							new BigDecimal(strArr[2]), unit);
					BigDecimal differentAmount = getMultiplied(new BigDecimal(
							strArr[3]), unit);
					BigDecimal differentPercent = new BigDecimal(strArr[4]);
					BigDecimal cumulativeAmountOfThisYear = getMultiplied(
							new BigDecimal(strArr[5]), unit);
					BigDecimal cumulativeAmountOfLastYear = getMultiplied(
							new BigDecimal(strArr[6]), unit);
					BigDecimal cumulativeDifferentAmount = getMultiplied(
							new BigDecimal(strArr[7]), unit);
					BigDecimal cumulativeDifferentPercent = new BigDecimal(
							strArr[8]);
					BigDecimal exchangeRateOfCurrentMonth = getBigDecimal(strArr[9]);
					BigDecimal cumulativeExchangeRateOfThisYear = getBigDecimal(strArr[10]);
					String comment = strArr[11];
					MonthlyOperatingIncome entity = generateEntity(stockCode,
							isFunctionalCurrency, currency, year, month, ver,
							currentMonth, currentMonthOfLastYear,
							differentAmount, differentPercent,
							cumulativeAmountOfThisYear,
							cumulativeAmountOfLastYear,
							cumulativeDifferentAmount,
							cumulativeDifferentPercent,
							exchangeRateOfCurrentMonth,
							cumulativeExchangeRateOfThisYear, comment);
					entities.add(entity);
				}
				incomeRepo.put(entities);
				logger.info(file.getName() + " saved to "
						+ incomeRepo.getTargetTableName() + ".");
				writeToProcessedLogAndProcessedSet(file);
				++count;
			}
		}
		return count;
	}

	private BigDecimal getBigDecimal(String str) {
		switch (str) {
		case EMPTY_STRING:
		case "─":
			return null;
		default:
			return new BigDecimal(str);
		}
	}

	// private void writeToProcessedFile(File file) throws IOException {
	// String infoLine = generateProcessedInfo(file) + System.lineSeparator();
	// FileUtils.write(processedLog, infoLine, Charsets.UTF_8, true);
	// }

	private MonthlyOperatingIncome generateEntity(String stockCode,
			boolean isFunctionalCurrency, CurrencyType currency, int year,
			int month, Date ver, BigDecimal currentMonth,
			BigDecimal currentMonthOfLastYear, BigDecimal differentAmount,
			BigDecimal differentPercent, BigDecimal cumulativeAmountOfThisYear,
			BigDecimal cumulativeAmountOfLastYear,
			BigDecimal cumulativeDifferentAmount,
			BigDecimal cumulativeDifferentPercent,
			BigDecimal exchangeRateOfCurrentMonth,
			BigDecimal cumulativeExchangeRateOfThisYear, String comment) {
		MonthlyOperatingIncome entity = new MonthlyOperatingIncome();
		entity.new RowKey(stockCode, isFunctionalCurrency, currency, year,
				month, entity);
		generateOperatingIncomeFamilyContent(entity, ver, currentMonth,
				currentMonthOfLastYear, differentAmount, differentPercent,
				cumulativeAmountOfThisYear, cumulativeAmountOfLastYear,
				cumulativeDifferentAmount, cumulativeDifferentPercent,
				exchangeRateOfCurrentMonth, cumulativeExchangeRateOfThisYear,
				comment);
		return entity;
	}

	private void generateOperatingIncomeFamilyContent(
			MonthlyOperatingIncome entity, Date ver, BigDecimal currentMonth,
			BigDecimal currentMonthOfLastYear, BigDecimal differentAmount,
			BigDecimal differentPercent, BigDecimal cumulativeAmountOfThisYear,
			BigDecimal cumulativeAmountOfLastYear,
			BigDecimal cumulativeDifferentAmount,
			BigDecimal cumulativeDifferentPercent,
			BigDecimal exchangeRateOfCurrentMonth,
			BigDecimal cumulativeExchangeRateOfThisYear, String comment) {
		IncomeFamily fam = entity.getIncomeFamily();
		fam.setCurrentMonth(ver, currentMonth);
		fam.setCurrentMonthOfLastYear(ver, currentMonthOfLastYear);
		fam.setDifferentAmount(ver, differentAmount);
		fam.setDifferentPercent(ver, differentPercent);
		fam.setCumulativeAmountOfThisYear(ver, cumulativeAmountOfThisYear);
		fam.setCumulativeAmountOfLastYear(ver, cumulativeAmountOfLastYear);
		fam.setCumulativeDifferentAmount(ver, cumulativeDifferentAmount);
		fam.setCumulativeDifferentPercent(ver, cumulativeDifferentPercent);
		fam.setExchangeRateOfCurrentMonth(ver, exchangeRateOfCurrentMonth);
		fam.setCumulativeExchangeRateOfThisYear(ver,
				cumulativeExchangeRateOfThisYear);
		fam.setComment(ver, comment);
	}

	private int getStartRow(File file, List<String> lines) {
		String targetStr = getTargetStartRowString();
		for (int i = 0, size = lines.size(); i < size; ++i) {
			if (targetStr.equals(lines.get(i))) {
				return i + 1;
			}
		}
		throw new RuntimeException("File(" + file.getAbsolutePath()
				+ ") cannot find line(" + targetStr + ") !!!");
	}

	private String getTargetStartRowString() {
		return "貨幣,本月,去年同期,增減金額,增減百分比,本年累計,去年累計,增減金額,增減百分比,本月換算匯率：,本年累計換算匯率：,備註";
	}

	private File downloadMonthlyOperatingIncome() {
		try {
			MonthlyOperatingIncomeDownloader downloader = applicationContext
					.getBean(MonthlyOperatingIncomeDownloader.class);
			File dir = downloader.downloadMonthlyOperatingIncome();
			logger.info(dir.getAbsolutePath() + " download finish.");
			return dir;
		} catch (Exception e) {
			logger.error("Download monthly operating income fail !!!");
			return null;
		}
	}

	// private void generateProcessedLog() throws IOException {
	// if (processedLog == null) {
	// processedLog = new File(downloadDir, "processed.log");
	// if (processedLog.exists() == false) {
	// FileUtils.touch(processedLog);
	// }
	// }
	// }

	private boolean isProcessed(File file) throws IOException {
		String processedInfo = generateProcessedInfo(file);
		if (processedSet.contains(processedInfo)) {
			logger.debug(processedInfo + " processed before.");
			return true;
		}
		return false;
	}

	private String generateProcessedInfo(File file) {
		return file.getName();
	}

	private void readProcessedLogAndUpdateProcessedSet(File dir)
			throws IOException {
		processedLog = FileUtility
				.getOrCreateFile(dir, PROCESSED_LOG_FILE_NAME);
		processedSet = FileUtility.readLinesAsHashSet(processedLog);
	}

	private void writeToProcessedLogAndProcessedSet(File file)
			throws IOException {
		String infoLine = generateProcessedInfo(file) + System.lineSeparator();
		FileUtils.write(processedLog, infoLine, Charsets.UTF_8, true);
		processedSet.add(infoLine);
	}
}

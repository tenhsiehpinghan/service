package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.collectionutility.utility.ArrayUtility;
import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.datetimeutility.utility.DateUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.seleniumassistant.browser.BrowserBase;
import idv.hsiehpinghan.seleniumassistant.browser.HtmlUnitFirefoxVersionBrowser;
import idv.hsiehpinghan.seleniumassistant.utility.AjaxWaitUtility;
import idv.hsiehpinghan.seleniumassistant.webelement.Div;
import idv.hsiehpinghan.seleniumassistant.webelement.Select;
import idv.hsiehpinghan.seleniumassistant.webelement.Td;
import idv.hsiehpinghan.seleniumassistant.webelement.TextInput;
import idv.hsiehpinghan.stockdao.entity.StockInfo.RowKey;
import idv.hsiehpinghan.stockdao.repository.StockInfoRepository;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.webelement.MonthlyOperatingIncomeDownloadTable;
import idv.hsiehpinghan.stockservice.webelement.MonthlyOperatingIncomeDownloadTable.MonthlyOperatingIncome;
import idv.hsiehpinghan.stockservice.webelement.SubsidiaryTable;
import idv.hsiehpinghan.threadutility.utility.ThreadUtility;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MonthlyOperatingIncomeDownloader implements InitializingBean {
	public static final String DOWNLOADED_LOG_FILE_NAME = "downloaded.log";
	private final String COMMA_STRING = StringUtility.COMMA_STRING;
	private final String EMPTY_STRING = StringUtility.EMPTY_STRING;
	private String titleString;
	private final String YYYYMM = "yyyyMM";
	private final String HISTORY = "歷史資料";
	private final int MAX_TRY_AMOUNT = 100;
	private final Date BEGIN_DATA_DATE = generateBeginDataDate();
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDir;
	private File downloadedLog;
	private Set<String> downloadedSet;
	private StringBuilder sb = new StringBuilder();

	@Autowired
	private StockServiceProperty stockServiceProperty;
	@Autowired
	private StockInfoRepository infoRepo;
	@Autowired
	private HtmlUnitFirefoxVersionBrowser browser;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDir = stockServiceProperty
				.getMonthlyOperatingIncomeDownloadDir();
	}

	public File downloadMonthlyOperatingIncome() throws IOException,
			IllegalAccessException, NoSuchMethodException, SecurityException,
			InstantiationException, IllegalArgumentException,
			InvocationTargetException {
		moveToTargetPage();
		Date now = Calendar.getInstance().getTime();
		selectSearchType(HISTORY);
		Date targetDate = BEGIN_DATA_DATE;
		// while (targetDate.getTime() < now.getTime()) {
		while (targetDate.getTime() < DateUtils.addMonths(now, -1).getTime()) {
			readDownloadedLogAndUpdateDownloadedSet(targetDate);
			for (RowKey rowKey : infoRepo.getRowKeys()) {
				String stockCode = rowKey.getStockCode();
				inputStockCode(stockCode);
				String downloadInfo = getDownloadInfo(stockCode, targetDate);
				if (isDownloaded(downloadInfo) == false) {
					inputYear(targetDate);
					selectMonth(targetDate);
					logger.info(downloadInfo + " process start.");
					boolean markAsDownloaded = repeatTryDownload(stockCode,
							targetDate);
					if (markAsDownloaded == true) {
						writeToDownloadedLogAndDownloadedSet(downloadInfo);
						logger.info(downloadInfo + " processed success.");
					} else {
						logger.info(downloadInfo + " has no data.");
					}

				}
			}
			targetDate = DateUtils.addMonths(targetDate, 1);
		}
		return downloadDir;
	}

	void moveToTargetPage() {
		final String MONTHLY_OPERATION_INCOME_PAGE_URL = "http://mops.twse.com.tw/mops/web/t05st10_ifrs";
		browser.browse(MONTHLY_OPERATION_INCOME_PAGE_URL);
		Div div = browser.getDiv(By.cssSelector("#caption"));
		AjaxWaitUtility.waitUntilTextStartWith(div, "   採用IFRSs後之月營業收入資訊");
	}

	BrowserBase getBrowser() {
		return browser;
	}

	void selectSearchType(String text) {
		Select sel = browser.getSelect(By.cssSelector("#isnew"));
		sel.selectByText(text);
		AjaxWaitUtility.waitUntilDisplayed(getYearDiv());
	}

	void inputStockCode(String stockCode) {
		TextInput input = browser.getTextInput(By.cssSelector("#co_id"));
		input.clear();
		input.inputText(stockCode);
	}

	void inputYear(Date date) {
		TextInput input = browser.getTextInput(By.cssSelector("#year"));
		input.clear();
		int year = DateUtility.getRocYear(date);
		input.inputText(String.valueOf(year));
	}

	void selectMonth(Date date) {
		Select sel = browser.getSelect(By.cssSelector("#month"));
		int month = DateUtility.getMonth(date);
		sel.selectByText(String.valueOf(month));
	}

	boolean repeatTryDownload(String stockCode, Date targetDate) {
		int tryAmount = 0;
		while (true) {
			try {
				browser.getButton(
						By.cssSelector("td.bar01b:nth-child(4) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > div:nth-child(1) > div:nth-child(1) > input:nth-child(1)"))
						.click();
				return download(stockCode, targetDate);
			} catch (Exception e) {
				++tryAmount;
				logger.info("Download fail " + tryAmount + " times !!!");
				if (tryAmount >= MAX_TRY_AMOUNT) {
					logger.error(browser.getWebDriver().getPageSource());
					throw new RuntimeException(e);
				}
				ThreadUtility.sleep(tryAmount * 1);
			}
		}
	}

	String getFileName(String stockCode, Date date) {
		return getDownloadInfo(stockCode, date) + ".csv";
	}

	File getTargetDirectory(Date date) {
		int year = DateUtility.getYear(date);
		int month = DateUtility.getMonth(date);
		return FileUtility.getOrCreateDirectory(downloadDir,
				String.valueOf(year), String.valueOf(month));
	}

	void readDownloadedLogAndUpdateDownloadedSet(Date date) throws IOException {
		File targetDir = getTargetDirectory(date);
		downloadedLog = FileUtility.getOrCreateFile(targetDir,
				DOWNLOADED_LOG_FILE_NAME);
		downloadedSet = FileUtility.readLinesAsHashSet(downloadedLog);
	}

	private String getTargetText(Date date) {
		String rocYear = DateUtility.getRocDateString(date, "yyyy");
		String month = DateUtility.getRocDateString(date, "MM");
		return "民國" + rocYear + "年" + month + "月";
	}

	private boolean download(String stockCode, Date targetDate)
			throws IOException {
		try {
			Td td = browser.getTd(By.cssSelector("td.reportCont:nth-child(2)"));
			AjaxWaitUtility.waitUntilTextEqual(td, getTargetText(targetDate));
			MonthlyOperatingIncomeDownloadTable tab = new MonthlyOperatingIncomeDownloadTable(
					browser.getTable(By
							.cssSelector("#table01 > table.hasBorder")));
			File file = writeToCsvFile(stockCode, targetDate, tab);
			logger.info(file.getAbsolutePath() + " downloaded.");
			return true;
		} catch (TimeoutException e) {
			try {
				String text = browser.getFont(
						By.cssSelector("#table01 > center > h3:nth-child(1)"))
						.getText();
				if ("資料庫中查無需求資料。".equals(text)) {
					Date now = Calendar.getInstance().getTime();
					if (isTwoMonthAgo(targetDate, now)) {
						return true;
					} else {
						return false;
					}
				} else if ("外國發行人免申報本項資訊".equals(text)) {
					return true;
				} else if ("本項目對發行臺灣存託憑證之公司屬自願申報".equals(text)) {
					return true;
				}
				throw e;
			} catch (NoSuchElementException ex) {
				return repeatTryDownloadSubsidiary(targetDate);
			}
		}
	}

	private SubsidiaryTable getSubsidiaryTable() {
		return new SubsidiaryTable(browser.getTable(By
				.cssSelector("#table01 > form > table.hasBorder")));
	}

	private boolean repeatTryDownloadSubsidiary(Date targetDate) {
		SubsidiaryTable tab = getSubsidiaryTable();
		// i = 0 is title.
		for (int i = tab.getRowSize() - 1, endRowIndex = 1; i >= endRowIndex; --i) {
			boolean hasClickedQueryButton = false;
			int tryAmount = 0;
			while (true) {
				hasClickedQueryButton = false;
				try {
					String stockCode = tab.getStockCode(i);
					String downloadInfo = getDownloadInfo(stockCode, targetDate);
					if (isDownloaded(downloadInfo) == false) {
						tab.clickQueryButton(i);
						hasClickedQueryButton = true;
						download(stockCode, targetDate);
						writeToDownloadedLogAndDownloadedSet(downloadInfo);
					}
					break;
				} catch (Exception e) {
					++tryAmount;
					logger.info("Subsidiary download fail " + tryAmount
							+ " times !!!");
					if (tryAmount >= MAX_TRY_AMOUNT) {
						logger.error(browser.getWebDriver().getPageSource());
						throw new RuntimeException(e);
					}
					ThreadUtility.sleep(tryAmount * 10);
				} finally {
					if (hasClickedQueryButton) {
						clickBackToPrePageButton();
					}
				}
			}
		}
		return true;
	}

	private void clickBackToPrePageButton() {
		browser.getA(By.cssSelector("#ajax_back_button")).click();
		SubsidiaryTable tab = getSubsidiaryTable();
		AjaxWaitUtility.waitUntilRowTextEqual(tab, 0, tab.getTargetRowTexts());
	}

	private File writeToCsvFile(String stockCode, Date date,
			MonthlyOperatingIncomeDownloadTable table) throws IOException {
		File csvFile = new File(getTargetDirectory(date), getFileName(
				stockCode, date));
		FileUtils.write(csvFile, generateCsvFileTitle(), Charsets.UTF_8, false);
		FileUtils.write(csvFile, generateCsvFileData(table), Charsets.UTF_8,
				true);
		return csvFile;
	}

	private String generateCsvFileTitle() {
		if (titleString == null) {
			titleString = "貨幣"
					+ COMMA_STRING
					+ ArrayUtility
							.toString(MonthlyOperatingIncomeDownloadTable
									.getItemNames2(), COMMA_STRING);
		}
		return titleString + System.lineSeparator();
	}

	private String generateCsvFileData(MonthlyOperatingIncomeDownloadTable table) {
		sb.setLength(0);
		for (Entry<String, MonthlyOperatingIncome> ent : table
				.getMonthlyOperatingIncomes().entrySet()) {
			String currency = ent.getKey();
			MonthlyOperatingIncome income = ent.getValue();
			String currentMonth = income.getCurrentMonth().replace(
					COMMA_STRING, EMPTY_STRING);
			String currentMonthOfLastYear = income.getCurrentMonthOfLastYear()
					.replace(COMMA_STRING, EMPTY_STRING);
			String differentAmount = income.getDifferentAmount().replace(
					COMMA_STRING, EMPTY_STRING);
			String differentPercent = income.getDifferentPercent().replace(
					COMMA_STRING, EMPTY_STRING);
			String cumulativeAmountOfThisYear = income
					.getCumulativeAmountOfThisYear().replace(COMMA_STRING,
							EMPTY_STRING);
			String cumulativeAmountOfLastYear = income
					.getCumulativeAmountOfLastYear().replace(COMMA_STRING,
							EMPTY_STRING);
			String cumulativeDifferentAmount = income
					.getCumulativeDifferentAmount().replace(COMMA_STRING,
							EMPTY_STRING);
			String cumulativeDifferentPercent = income
					.getCumulativeDifferentPercent().replace(COMMA_STRING,
							EMPTY_STRING);
			String exchangeRateOfCurrentMonth = income
					.getExchangeRateOfCurrentMonth().replace(COMMA_STRING,
							EMPTY_STRING);
			String cumulativeExchangeRateOfCurrentYear = income
					.getCumulativeExchangeRateOfCurrentYear().replace(
							COMMA_STRING, EMPTY_STRING);
			String comment = income.getComment();
			sb.append(currency);
			sb.append(COMMA_STRING);
			sb.append(currentMonth);
			sb.append(COMMA_STRING);
			sb.append(currentMonthOfLastYear);
			sb.append(COMMA_STRING);
			sb.append(differentAmount);
			sb.append(COMMA_STRING);
			sb.append(differentPercent);
			sb.append(COMMA_STRING);
			sb.append(cumulativeAmountOfThisYear);
			sb.append(COMMA_STRING);
			sb.append(cumulativeAmountOfLastYear);
			sb.append(COMMA_STRING);
			sb.append(cumulativeDifferentAmount);
			sb.append(COMMA_STRING);
			sb.append(cumulativeDifferentPercent);
			sb.append(COMMA_STRING);
			sb.append(exchangeRateOfCurrentMonth);
			sb.append(COMMA_STRING);
			sb.append(cumulativeExchangeRateOfCurrentYear);
			sb.append(COMMA_STRING);
			sb.append(comment);
			sb.append(System.lineSeparator());
		}
		return sb.toString();
	}

	private Date generateBeginDataDate() {
		return DateUtility.getDate(2013, 11, 1);
	}

	private String getDownloadInfo(String stockCode, Date date) {
		return stockCode + "_" + DateFormatUtils.format(date, YYYYMM);
	}

	private boolean isDownloaded(String downloadInfo) throws IOException {
		if (downloadedSet.contains(downloadInfo)) {
			logger.debug(downloadInfo + " downloaded before.");
			return true;
		}
		return false;
	}

	private boolean isTwoMonthAgo(Date targetDate, Date now) {
		long diff = now.getTime() - targetDate.getTime();
		long twoMonth = 60 * DateUtility.DAY_MILLISECONDS;
		return diff > twoMonth;
	}

	private void writeToDownloadedLogAndDownloadedSet(String downloadInfo)
			throws IOException {
		String infoLine = downloadInfo + System.lineSeparator();
		FileUtils.write(downloadedLog, infoLine, Charsets.UTF_8, true);
		downloadedSet.add(downloadInfo);
	}

	private Div getYearDiv() {
		return browser.getDiv(By.cssSelector("#year"));
	}
}

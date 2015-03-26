package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datatypeutility.utility.CharsetUtility;
import idv.hsiehpinghan.datetimeutility.utility.DateUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.seleniumassistant.browser.BrowserBase;
import idv.hsiehpinghan.seleniumassistant.browser.HtmlUnitFirefoxVersionBrowser;
import idv.hsiehpinghan.seleniumassistant.utility.AjaxWaitUtility;
import idv.hsiehpinghan.seleniumassistant.webelement.Div;
import idv.hsiehpinghan.seleniumassistant.webelement.Select;
import idv.hsiehpinghan.seleniumassistant.webelement.TextInput;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.utility.StockAjaxWaitUtility;
import idv.hsiehpinghan.stockservice.webelement.GretaiDatePickerTable;
import idv.hsiehpinghan.threadutility.utility.ThreadUtility;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StockClosingConditionOfGretaiDownloader implements
		InitializingBean {
	private final Charset UTF_8 = CharsetUtility.UTF_8;
	private final String YYYYMMDD = "yyyyMMdd";
	private final String ALL = "所有證券(不含權證、牛熊證)";
	private final int MAX_TRY_AMOUNT = 3;
	private final Date BEGIN_DATA_DATE = DateUtility.getDate(2015, 1, 1);
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDir;
	private File downloadedLog;
	private Set<String> downloadedSet;

	@Autowired
	private HtmlUnitFirefoxVersionBrowser browser;
	// private FirefoxBrowser browser = new FirefoxBrowser();

	@Autowired
	private StockServiceProperty stockServiceProperty;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDir = stockServiceProperty
				.getStockClosingConditionDownloadDirOfGretai();
		generateDownloadedLogFile();
	}

	public File downloadStockClosingCondition() throws IOException {
		moveToTargetPage();
		downloadedSet = FileUtility.readLinesAsHashSet(downloadedLog);
		Date now = Calendar.getInstance().getTime();
		Date targetDate = BEGIN_DATA_DATE;
		while (targetDate.getTime() < now.getTime()) {
			String downloadInfo = getDownloadInfo(targetDate);
			if (isDownloaded(downloadInfo) == false) {
				selectType(ALL);
				inputDataDate(targetDate);
				logger.info(downloadInfo + " process start.");
				repeatTryDownload(targetDate);
				logger.info(downloadInfo + " processed success.");
				writeToDownloadedFileAndSet(downloadInfo);
			}
			targetDate = DateUtils.addDays(targetDate, 1);
		}
		return downloadDir;
	}

	void moveToTargetPage() {
		final String STOCK_CLOSING_CONDITION_PAGE_URL = "http://www.gretai.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw";
		browser.browse(STOCK_CLOSING_CONDITION_PAGE_URL);
		Div div = browser
				.getDiv(By
						.cssSelector("body > div:nth-child(1) > div.h-pnl.rpt-title-fullscreen"));
		AjaxWaitUtility.waitUntilTextStartWith(div, "  上櫃股票每日收盤行情(不含定價)");
	}

	BrowserBase getBrowser() {
		return browser;
	}

	void inputDataDate(Date date) {
		triggerDatepickerDisplay();
		GretaiDatePickerTable table = new GretaiDatePickerTable(
				browser.getTable(By.cssSelector("#ui-datepicker-div > table")));
		selectYear(date, table);
		selectMonth(date, table);
		selectDayOfMonth(date, table);
	}

	void selectType(String text) {
		Select typeSel = browser.getSelect(By.cssSelector("#sect"));
		typeSel.selectByText(text);
	}

	void repeatTryDownload(Date targetDate) {
		int tryAmount = 0;
		while (true) {
			try {
				downloadCsv(targetDate);
				break;
			} catch (Exception e) {
				++tryAmount;
				logger.info("Download fail " + tryAmount + " times !!!");
				if (tryAmount >= MAX_TRY_AMOUNT) {
					logger.error(browser.getWebDriver().getPageSource());
					throw new RuntimeException(e);
				}
				ThreadUtility.sleep(tryAmount * 10);
			}
		}
	}

	String getFileName(String str) {
		int idxBegin = str.indexOf("=") + 1;
		return str.substring(idxBegin);
	}

	private void downloadCsv(Date targetDate) {
		browser.closeAllChildWindow();
		browser.getButton(
				By.cssSelector("body > div:nth-child(1) > div.h-pnl-right.rpt-search-fullscreen > button.btn-download.ui-button.ui-widget.ui-state-default.ui-corner-all.ui-button-text-icon-primary"))
				.click();
		if (browser.hasChildWindow() == false) {
			return;
		}
		AjaxWaitUtility.waitUntilFirstChildWindowAttachmentNotNull(browser);
		browser.switchToFirstChildWindow();
		try {
			String fileName = getFileName(browser.getAttachment());
			File file = new File(downloadDir.getAbsolutePath(), fileName);
			browser.download(file);
			logger.info(file.getAbsolutePath() + " downloaded.");
		} finally {
			browser.switchToParentWindow();
			browser.closeAllChildWindow();
		}
	}

	private void generateDownloadedLogFile() throws IOException {
		if (downloadedLog == null) {
			downloadedLog = new File(downloadDir, "downloaded.log");
			if (downloadedLog.exists() == false) {
				FileUtils.touch(downloadedLog);
			}
		}
	}

	private String getDownloadInfo(Date date) {
		return DateFormatUtils.format(date, YYYYMMDD);
	}

	private boolean isDownloaded(String downloadInfo) throws IOException {
		if (downloadedSet.contains(downloadInfo)) {
			logger.info(downloadInfo + " downloaded before.");
			return true;
		}
		return false;
	}

	private void writeToDownloadedFileAndSet(String downloadInfo)
			throws IOException {
		String infoLine = downloadInfo + System.lineSeparator();
		FileUtils.write(downloadedLog, infoLine, UTF_8, true);
		downloadedSet.add(downloadInfo);
	}

	private void triggerDatepickerDisplay() {
		TextInput dataDateInput = browser.getTextInput(By
				.cssSelector("#input_date"));
		dataDateInput.click();
		Div datepickerDiv = browser
				.getDiv(By.cssSelector("#ui-datepicker-div"));
		AjaxWaitUtility.waitUntilDisplayed(datepickerDiv);
	}

	private void selectYear(Date date, GretaiDatePickerTable table) {
		// Input year.
		Select yearSel = browser
				.getSelect(By
						.cssSelector("#ui-datepicker-div > div > div > select.ui-datepicker-year"));
		yearSel.click();
		int year = DateUtility.getYear(date);
		yearSel.selectByValue(String.valueOf(year));
		StockAjaxWaitUtility.waitUntilAllDataYearEqual(table, year);
	}

	private void selectMonth(Date date, GretaiDatePickerTable table) {
		// Input month.
		Select monthSel = browser
				.getSelect(By
						.cssSelector("#ui-datepicker-div > div > div > select.ui-datepicker-month"));
		monthSel.click();
		int year = DateUtility.getYear(date);
		// Month value begins from 0.
		int month = DateUtility.getMonth(date) - 1;
		monthSel.selectByValue(String.valueOf(month));
		StockAjaxWaitUtility.waitUntilAllDataYearAndDataMonthEqual(table, year,
				month);
	}

	private void selectDayOfMonth(Date date, GretaiDatePickerTable table) {
		int dayOfMonth = DateUtility.getDayOfMonth(date);
		// Input day of month.
		table.clickDayOfMonth(dayOfMonth);
		TextInput dateInput = browser.getTextInput(By
				.cssSelector("#input_date"));
		String targetDateStr = DateUtility.getRocDateString(date, "yyyy/MM/dd");
		AjaxWaitUtility.waitUntilValueEqual(dateInput, targetDateStr);
	}
}

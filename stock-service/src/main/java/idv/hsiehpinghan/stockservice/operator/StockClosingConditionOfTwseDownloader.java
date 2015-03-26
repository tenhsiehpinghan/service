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
public class StockClosingConditionOfTwseDownloader implements InitializingBean {
	private final Charset UTF_8 = CharsetUtility.UTF_8;
	private final String YYYYMMDD = "yyyyMMdd";
	private final String ALL = "全部(不含權證、牛熊證、可展延牛熊證)";
	private final int MAX_TRY_AMOUNT = 3;
	private final Date BEGIN_DATA_DATE = DateUtility.getDate(2015, 1, 1);
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDir;
	private File downloadedLog;
	private Set<String> downloadedSet;

	@Autowired
	private HtmlUnitFirefoxVersionBrowser browser;
	// private HtmlUnitWithJavascriptBrowser browser;
	// private FirefoxBrowser browser;
	@Autowired
	private StockServiceProperty stockServiceProperty;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDir = stockServiceProperty
				.getStockClosingConditionDownloadDirOfTwse();
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
				inputDataDate(targetDate);
				selectType(ALL);
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
		final String STOCK_CLOSING_CONDITION_PAGE_URL = "http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php";
		browser.browse(STOCK_CLOSING_CONDITION_PAGE_URL);
		Div div = browser.getDiv(By.id("breadcrumbs"));
		AjaxWaitUtility
				.waitUntilTextStartWith(div, "首頁 > 交易資訊 > 盤後資訊 > 每日收盤行情");
	}

	BrowserBase getBrowser() {
		return browser;
	}

	void inputDataDate(Date date) {
		TextInput dataDateInput = browser.getTextInput(By.id("date-field"));
		dataDateInput.clear();
		String dateStr = DateUtility.getRocDateString(date, "yyyy/MM/dd");
		dataDateInput.inputText(dateStr);
	}

	void selectType(String text) {
		Select typeSel = browser.getSelect(By
				.cssSelector("#main-content > form > select"));
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
		browser.cacheCurrentPage();
		try {
			browser.getButton(By.cssSelector(".dl-csv")).click();
			String fileName = getFileName(browser.getAttachment());
			File file = new File(downloadDir.getAbsolutePath(), fileName);
			browser.download(file);
			logger.info(file.getAbsolutePath() + " downloaded.");
			browser.restorePage();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			browser.restorePage();
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
}

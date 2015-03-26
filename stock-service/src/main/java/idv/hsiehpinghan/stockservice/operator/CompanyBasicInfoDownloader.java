package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.seleniumassistant.browser.BrowserBase;
import idv.hsiehpinghan.seleniumassistant.browser.HtmlUnitFirefoxVersionBrowser;
import idv.hsiehpinghan.seleniumassistant.utility.AjaxWaitUtility;
import idv.hsiehpinghan.seleniumassistant.webelement.Button;
import idv.hsiehpinghan.seleniumassistant.webelement.Div;
import idv.hsiehpinghan.seleniumassistant.webelement.Font;
import idv.hsiehpinghan.seleniumassistant.webelement.Select;
import idv.hsiehpinghan.seleniumassistant.webelement.Select.Option;
import idv.hsiehpinghan.seleniumassistant.webelement.Td;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.threadutility.utility.ThreadUtility;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CompanyBasicInfoDownloader implements InitializingBean {
	private final String EMPTY_STRING = StringUtility.EMPTY_STRING;
	private final int MAX_TRY_AMOUNT = 10;
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDir;
	private File downloadedLog;
	private Set<String> downloadedSet;
	@Autowired
	private HtmlUnitFirefoxVersionBrowser browser;
	@Autowired
	private StockServiceProperty stockServiceProperty;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDir = stockServiceProperty.getCompanyBasicInfoDownloadDir();
		generateDownloadedLogFile();
	}

	public File downloadCompanyBasicInfo() throws IOException {
		moveToTargetPage();
		downloadedSet = FileUtility.readLinesAsHashSet(downloadedLog);
		List<Option> mkOpts = getMarketTypeSelect().getOptions();
		String oldStockCode = EMPTY_STRING;
		for (int iMk = mkOpts.size() - 1; iMk >= 0; --iMk) {
			Option mkOpt = mkOpts.get(iMk);
			if (isTargetMarketType(mkOpt.getText().trim()) == false) {
				continue;
			}
			List<Option> oldIndOpts = getIndustryTypeSelect().getOptions();
			mkOpt.click();
			List<Option> indOpts = getIndustryOptions(oldIndOpts);
			for (int iInd = indOpts.size() - 1; iInd >= 0; --iInd) {
				Option indOpt = indOpts.get(iInd);
				if (isTargetIndustryType(indOpt.getText().trim()) == false) {
					continue;
				}
				String downloadInfo = getDownloadInfo(mkOpt, indOpt);
				if (isDownloaded(downloadInfo)) {
					continue;
				}
				indOpt.click();
				// ex : sii_03.csv
				String fileName = getFileName(mkOpt, indOpt);
				logger.info(downloadInfo + " process start.");
				oldStockCode = repeatTryDownload(oldStockCode, fileName);
				if (EMPTY_STRING.equals(oldStockCode) == false) {
					logger.info(downloadInfo + " processed success.");
					writeToDownloadedFileAndSet(downloadInfo);
				} else {
					logger.info(downloadInfo + " has no data.");
				}

			}
		}
		return downloadDir;
	}

	void moveToTargetPage() {
		final String COMPANY_BASIC_INFO_PAGE_URL = "http://mops.twse.com.tw/mops/web/t51sb01";
		browser.browse(COMPANY_BASIC_INFO_PAGE_URL);
		Div div = browser.getDiv(By.cssSelector("#caption"));
		AjaxWaitUtility.waitUntilTextStartWith(div, "   基本資料查詢彙總表");
	}

	BrowserBase getBrowser() {
		return browser;
	}

	String repeatTryDownload(String oldStockCode, String fileName) {
		int tryAmount = 0;
		while (true) {
			try {
				getSearchButton().click();
				String newStockCode = waitAjaxTableReload(oldStockCode);
				// Null means "查無資料！"
				if (newStockCode == null) {
					return EMPTY_STRING;
				}
				downLoad(fileName);
				return newStockCode;
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

	private Select getMarketTypeSelect() {
		return browser
				.getSelect(By
						.cssSelector("#search > table > tbody > tr > td > select:nth-child(2)"));
	}

	private void generateDownloadedLogFile() throws IOException {
		if (downloadedLog == null) {
			downloadedLog = new File(downloadDir, "downloaded.log");
			if (downloadedLog.exists() == false) {
				FileUtils.touch(downloadedLog);
			}
		}
	}

	private boolean isTargetMarketType(String text) {
		if ("上市".equals(text) == true) {
			return true;
		}
		if ("上櫃".equals(text) == true) {
			return true;
		}
		return false;
	}

	private boolean isTargetIndustryType(String text) {
		if ("".equals(text)) {
			return false;
		}
		// "化學生技醫療" = "化學工業" + "生技醫療業"
		if ("化學生技醫療".equals(text)) {
			return false;
		}
		return true;
	}

	private List<Option> getIndustryOptions(List<Option> oldIndustryOptions) {
		Select sel = getIndustryTypeSelect();
		AjaxWaitUtility.waitUntilOptionsDifferent(sel, oldIndustryOptions);
		return sel.getOptions();
	}

	private Select getIndustryTypeSelect() {
		return browser
				.getSelect(By
						.cssSelector("#search > table > tbody > tr > td > select:nth-child(4)"));
	}

	private Button getSearchButton() {
		return browser.getButton(By
				.cssSelector("#search_bar1 > div > input[type='button']"));
	}

	private void downLoad(String fileName) {
		browser.cacheCurrentPage();
		try {
			browser.getButton(
					By.cssSelector("#table01 > form:nth-child(3) > button"))
					.click();
			if (browser.hasAttachment() == false) {
				throw new RuntimeException("No attachment !!!");
			}
			File file = new File(downloadDir.getAbsolutePath(), fileName);
			browser.download(file);
			logger.info(file.getAbsolutePath() + " downloaded.");
		} finally {
			browser.restorePage();
		}
	}

	private String waitAjaxTableReload(String oldStockCode) {
		Td td = browser
				.getTd(By
						.cssSelector("#table01 > table:nth-child(4) > tbody > tr:nth-child(2) > td:nth-child(1)"));
		try {
			AjaxWaitUtility.waitUntilTextDifferent(td, oldStockCode);
			return td.getText();
		} catch (TimeoutException e) {
			Font font = browser.getFont(By
					.cssSelector("#table01 > center > font"));
			if ("查無資料！".equals(font.getText().trim())) {
				return null;
			}
			throw e;
		}
	}

	private String getFileName(Option mkOpt, Option indOpt) {
		return mkOpt.getValue() + "_" + indOpt.getValue() + ".csv";
	}

	private String getDownloadInfo(Option mkOpt, Option indOpt) {
		return mkOpt.getValue() + "/" + indOpt.getValue();
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
		FileUtils.write(downloadedLog, infoLine, Charsets.UTF_8, true);
		downloadedSet.add(downloadInfo);
	}
}

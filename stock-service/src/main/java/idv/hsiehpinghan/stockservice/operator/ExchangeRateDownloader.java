package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datetimeutility.utility.CalendarUtility;
import idv.hsiehpinghan.seleniumassistant.browser.HtmlUnitFirefoxVersionBrowser;
import idv.hsiehpinghan.seleniumassistant.webelement.Select;
import idv.hsiehpinghan.seleniumassistant.webelement.Select.Option;
import idv.hsiehpinghan.stockdao.enumeration.CurrencyType;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.webelement.ExchangeRateDownloadTable;
import idv.hsiehpinghan.threadutility.utility.ThreadUtility;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.google.common.base.Charsets;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExchangeRateDownloader implements InitializingBean {
	private final int MAX_TRY_AMOUNT = 3;
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDir;

	@Autowired
	private HtmlUnitFirefoxVersionBrowser browser;
	@Autowired
	private StockServiceProperty stockServiceProperty;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDir = stockServiceProperty.getExchangeRateDownloadDir();
	}

	/**
	 * Download exchange rate.
	 * 
	 * @param targetDallars
	 * @return
	 */
	public File downloadExchangeRate(List<CurrencyType> targetDallars) {
		moveToTargetPage();
		clickPeriodType();
		List<Option> yearOpts = getYearSelect().getOptions();
		for (int iYear = yearOpts.size() - 1; iYear >= 0; --iYear) {
			Option yearOpt = yearOpts.get(iYear);
			yearOpt.click();
			List<Option> monOpts = getMonthSelect().getOptions();
			for (int iMon = 0, size = monOpts.size(); iMon < size; ++iMon) {
				Option monOpt = monOpts.get(iMon);
				monOpt.click();
				clickItemType();
				List<Option> dolOpts = getDollarSelect().getOptions();
				for (int iDol = 0, dolSize = dolOpts.size(); iDol < dolSize; ++iDol) {
					Option dolOpt = dolOpts.get(iDol);
					String dollar = dolOpt.getValue();
					if (isTargetDollar(dollar, targetDallars) == false) {
						continue;
					}
					dolOpt.click();
					List<Option> comDolOpts = getCompareDollarSelect()
							.getOptions();
					for (int iComDol = 0, comDolSize = comDolOpts.size(); iComDol < comDolSize; ++iComDol) {
						Option comDolOpt = comDolOpts.get(iComDol);
						if ("".equals(comDolOpt.getValue()) == false) {
							continue;
						}
						String year = yearOpt.getValue();
						String month = monOpt.getValue();
						if (isAfterCurrentMonth(year, month)) {
							continue;
						}
						if (isBeforePreviousMonth(year, month)
								&& isDownloaded(year, month, dollar)) {
							logger.info(getTargetFile(year, month, dollar)
									.getAbsolutePath() + " downloaded before.");
							continue;
						}
						clickExchangeType();
						browser.cacheCurrentPage();
						clickQueryButton();
						repeatTryDownload(year, month, dollar);
						browser.restorePage();
					}
				}
			}
		}
		return downloadDir;
	}

	private boolean isAfterCurrentMonth(String year, String month) {
		int dataYyyyMm = Integer.valueOf(year + month);
		Calendar cal = Calendar.getInstance();
		int curMonthYyyyMm = CalendarUtility.getYyyyMm(cal);
		if (curMonthYyyyMm < dataYyyyMm) {
			return true;
		}
		return false;
	}

	private boolean isBeforePreviousMonth(String year, String month) {
		int dataYyyyMm = Integer.valueOf(year + month);
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -1);
		int preMonthYyyyMm = CalendarUtility.getYyyyMm(cal);
		if (dataYyyyMm < preMonthYyyyMm) {
			return true;
		}
		return false;
	}

	private boolean isDownloaded(String year, String month, String dollar) {
		File f = getTargetFile(year, month, dollar);
		if (f.exists()) {
			return true;
		}
		return false;
	}

	private void clickPeriodType() {
		browser.getRadio(By.id("view1_7")).click();
	}

	private void clickItemType() {
		browser.getRadio(By.id("afterOrNot0")).click();
	}

	private void clickExchangeType() {
		browser.getRadio(By.id("entity1")).click();
	}

	private void clickQueryButton() {
		browser.getButton(By.id("Button1")).click();
	}

	private boolean isTargetDollar(String dollar,
			List<CurrencyType> targetDallars) {
		for (CurrencyType dol : targetDallars) {
			if (dol.name().equals(dollar)) {
				return true;
			}
		}
		return false;
	}

	private Select getYearSelect() {
		return browser.getSelect(By.id("year"));
	}

	private Select getDollarSelect() {
		return browser.getSelect(By.id("whom1"));
	}

	private Select getCompareDollarSelect() {
		return browser.getSelect(By.id("whom2"));
	}

	private void moveToTargetPage() {
		final String FINANCIAL_REPORT_PAGE_URL = "http://rate.bot.com.tw/Pages/UIP004/UIP004INQ1.aspx?lang=zh-TW";
		browser.browse(FINANCIAL_REPORT_PAGE_URL);
	}

	private Select getMonthSelect() {
		return browser.getSelect(By.id("month"));
	}

	private File getTargetFile(String year, String month, String dollar) {
		return new File(downloadDir + "/" + dollar + "/" + year + "_" + month);
	}

	private void downLoad(String year, String month, String dollar,
			ExchangeRateDownloadTable table) throws ParseException, IOException {
		StringBuilder sb = new StringBuilder();
		// i = 0,1 is title.
		for (int i = 2, size = table.getRowSize(); i < size; ++i) {
			String date = table.getDate(i);
			sb.append(date);
			sb.append("\t");
			String rate = table.getExchangeRate(i);
			sb.append(rate);
			sb.append("\n");
		}
		File targetFile = getTargetFile(year, month, dollar);
		FileUtils.writeStringToFile(targetFile, sb.toString(), Charsets.UTF_8,
				false);
	}

	private ExchangeRateDownloadTable getTargetTable() {
		ExchangeRateDownloadTable tab;
		tab = new ExchangeRateDownloadTable(
				browser.getTable(By
						.cssSelector(".middle > center:nth-child(1) > table:nth-child(6)")));
		checkTableTitle(tab);
		return tab;
	}

	private void checkTableTitle(ExchangeRateDownloadTable table) {
		List<String> row0 = table.getRowAsStringList(0);
		List<String> expectedRow0 = table.getTargetRow0Texts();
		if (ListUtils.isEqualList(row0, expectedRow0) == false) {
			throw new RuntimeException("Table row 0 title(" + row0
					+ ") is different !!!");
		}
		List<String> row1 = table.getRowAsStringList(1);
		List<String> expectedRow1 = table.getTargetRow1Texts();
		if (ListUtils.isEqualList(row1, expectedRow1) == false) {
			throw new RuntimeException("Table row 1 title(" + row1
					+ ") is different !!!");
		}
	}

	private void repeatTryDownload(String year, String month, String dollar) {
		int tryAmount = 0;
		while (true) {
			try {
				ExchangeRateDownloadTable tab = getTargetTable();
				downLoad(year, month, dollar, tab);
				logger.info(getTargetFile(year, month, dollar)
						.getAbsolutePath() + " downloaded success.");
				break;
			} catch (Exception e) {
				++tryAmount;
				logger.info("Download fail " + tryAmount + " times !!!");
				if (tryAmount >= MAX_TRY_AMOUNT) {
					logger.error(browser.getWebDriver().getPageSource());
					throw new RuntimeException(e);
				}
				ThreadUtility.sleep(tryAmount * 60);
			}
		}
	}
}

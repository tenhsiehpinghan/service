package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datetimeutility.utility.DateUtility;
import idv.hsiehpinghan.seleniumassistant.webelement.TextInput;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;
import idv.hsiehpinghan.testutility.utility.DeleteUtility;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang3.ArrayUtils;
import org.openqa.selenium.By;
import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StockClosingConditionOfGretaiDownloaderTest {
	private StockServiceProperty stockServiceProperty;
	private StockClosingConditionOfGretaiDownloader downloaderOfGretai;
	private Date date = DateUtility.getDate(2013, 5, 8);

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		stockServiceProperty = applicationContext
				.getBean(StockServiceProperty.class);
		downloaderOfGretai = applicationContext
				.getBean(StockClosingConditionOfGretaiDownloader.class);
	}

	@Test
	public void moveToTargetPage() {
		downloaderOfGretai.moveToTargetPage();
		String text = downloaderOfGretai
				.getBrowser()
				.getDiv(By
						.cssSelector("body > div:nth-child(1) > div.h-pnl.rpt-title-fullscreen"))
				.getText();
		Assert.assertEquals(text, "  上櫃股票每日收盤行情(不含定價)");
	}

	@Test(dependsOnMethods = { "moveToTargetPage" })
	public void inputDataDate() {
		downloaderOfGretai.inputDataDate(date);
		TextInput dataDateInput = downloaderOfGretai.getBrowser().getTextInput(
				By.cssSelector("#input_date"));
		String actual = dataDateInput.getValue();
		String expected = DateUtility.getRocDateString(date, "yyyy/MM/dd");
		Assert.assertEquals(actual, expected);
	}

	@Test(dependsOnMethods = { "inputDataDate" })
	public void repeatTryDownload() {
		try {
			downloaderOfGretai.repeatTryDownload(date);
		} catch (Exception e) {
			System.err.println(downloaderOfGretai.getBrowser().getWebDriver()
					.getPageSource());
			throw new RuntimeException(e);
		}
		String dateStr = DateUtility.getRocDateString(date, "yyyyMMdd");
		File dir = stockServiceProperty
				.getStockClosingConditionDownloadDirOfGretai();
		String fileName = "SQUOTE_02_" + dateStr + ".csv";
		Assert.assertTrue(ArrayUtils.contains(dir.list(), fileName));
		DeleteUtility.delete(dir, fileName);
	}

	@Test(dependsOnMethods = { "repeatTryDownload" })
	public void downloadStockClosingCondition() throws Exception {
		File dir = downloaderOfGretai.downloadStockClosingCondition();
		String dateStr = DateUtility.getRocDateString(
				DateUtility.getDate(2013, 1, 2), "yyyyMMdd");
		String fileName = "SQUOTE_EW_" + dateStr + ".csv";
		Assert.assertTrue(ArrayUtils.contains(dir.list(), fileName));
	}
}
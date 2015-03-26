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
import org.apache.commons.lang3.time.DateFormatUtils;
import org.openqa.selenium.By;
import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StockClosingConditionOfTwseDownloaderTest {
	private StockServiceProperty stockServiceProperty;
	private StockClosingConditionOfTwseDownloader downloaderOfTwse;
	private Date date = DateUtility.getDate(2013, 1, 2);

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		stockServiceProperty = applicationContext
				.getBean(StockServiceProperty.class);
		downloaderOfTwse = applicationContext
				.getBean(StockClosingConditionOfTwseDownloader.class);
	}

	@Test
	public void moveToTargetPage() {
		downloaderOfTwse.moveToTargetPage();
		String breadcrumbs = downloaderOfTwse.getBrowser()
				.getDiv(By.cssSelector("#breadcrumbs")).getText();
		Assert.assertTrue(breadcrumbs.trim().startsWith(
				"首頁 > 交易資訊 > 盤後資訊 > 每日收盤行情"));
	}

	@Test(dependsOnMethods = { "moveToTargetPage" })
	public void inputDataDate() {
		downloaderOfTwse.inputDataDate(date);
		TextInput dataDateInput = downloaderOfTwse.getBrowser().getTextInput(
				By.id("date-field"));
		String actual = dataDateInput.getValue();
		String expected = DateUtility.getRocDateString(date, "yyyy/MM/dd");
		Assert.assertEquals(actual, expected);
	}

	@Test(dependsOnMethods = { "inputDataDate" })
	public void repeatTryDownload() {
		try {
			downloaderOfTwse.repeatTryDownload(date);
		} catch (Exception e) {
			System.err.println(downloaderOfTwse.getBrowser().getWebDriver()
					.getPageSource());
			throw new RuntimeException(e);
		}
		String dateStr = DateFormatUtils.format(date, "yyyyMMdd");
		File dir = stockServiceProperty
				.getStockClosingConditionDownloadDirOfTwse();
		String fileName = "A112" + dateStr + "MS.csv";
		Assert.assertTrue(ArrayUtils.contains(dir.list(), fileName));
		DeleteUtility.delete(dir, fileName);
	}

	@Test(dependsOnMethods = { "repeatTryDownload" })
	public void downloadStockClosingCondition() throws Exception {
		File dir = downloaderOfTwse.downloadStockClosingCondition();
		String dateStr = DateFormatUtils.format(date, "yyyyMMdd");
		String fileName = "A112" + dateStr + "ALLBUT0999.csv";
		Assert.assertTrue(ArrayUtils.contains(dir.list(), fileName));
	}

	@Test
	public void getFileName() {
		String str = "attachment; filename=A11220130102MS2.csv";
		Assert.assertEquals(downloaderOfTwse.getFileName(str),
				"A11220130102MS2.csv");
	}

}

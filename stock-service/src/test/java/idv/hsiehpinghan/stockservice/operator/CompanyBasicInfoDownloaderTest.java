package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;
import idv.hsiehpinghan.testutility.utility.DeleteUtility;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.openqa.selenium.By;
import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CompanyBasicInfoDownloaderTest {
	private StockServiceProperty stockServiceProperty;
	private CompanyBasicInfoDownloader downloader;

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		stockServiceProperty = applicationContext
				.getBean(StockServiceProperty.class);
		downloader = applicationContext
				.getBean(CompanyBasicInfoDownloader.class);
	}

	@Test
	public void moveToTargetPage() {
		downloader.moveToTargetPage();
		String caption = downloader.getBrowser()
				.getDiv(By.cssSelector("#caption")).getText();
		Assert.assertEquals(caption, "   基本資料查詢彙總表");
	}

	@Test(dependsOnMethods = { "moveToTargetPage" })
	public void repeatTryDownload() {
		String fileName = "test_file.csv";
		try {
			downloader.repeatTryDownload("", fileName);
		} catch (Exception e) {
			System.err.println(downloader.getBrowser().getWebDriver()
					.getPageSource());
			throw new RuntimeException(e);
		}
		File dir = stockServiceProperty.getCompanyBasicInfoDownloadDir();
		boolean result = ArrayUtils.contains(dir.list(), fileName);
		if (result == false) {
			System.err.println(downloader.getBrowser().getWebDriver()
					.getPageSource());
		}
		Assert.assertTrue(result);
		DeleteUtility.delete(dir, fileName);
	}

	@Test(dependsOnMethods = { "repeatTryDownload" })
	public void downloadCompanyBasicInfo() throws Exception {
		File dir = downloader.downloadCompanyBasicInfo();
		String fileName = "sii_01.csv";
		boolean result = ArrayUtils.contains(dir.list(), fileName);
		if (result == false) {
			System.err.println(downloader.getBrowser().getWebDriver()
					.getPageSource());
		}
		Assert.assertTrue(result);
	}
}

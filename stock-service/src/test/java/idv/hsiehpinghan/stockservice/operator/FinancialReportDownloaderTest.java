package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.seleniumassistant.browser.BrowserBase;
import idv.hsiehpinghan.seleniumassistant.webelement.Select;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.openqa.selenium.By;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FinancialReportDownloaderTest {
	private FinancialReportDownloader downloader;

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		downloader = applicationContext
				.getBean(FinancialReportDownloader.class);
	}

	@Test
	public void moveToTargetPage() {
		downloader.moveToTargetPage();
		BrowserBase browser = downloader.getBrowser();
		String capText = browser.getDiv(By.cssSelector("#caption")).getText();
		Assert.assertEquals("單一產業案例文件下載", capText.trim());
	}

	@Test(dependsOnMethods = { "moveToTargetPage" })
	public void getMarketTypeSelect() {
		Select select = downloader.getMarketTypeSelect();
		Assert.assertEquals(4, select.getOptions().size());
	}

	@Test(dependsOnMethods = { "getMarketTypeSelect" })
	public void downloadFinancialReport() throws IOException {
		File f = downloader.downloadFinancialReport();
		Assert.assertNotNull(f);
	}

}

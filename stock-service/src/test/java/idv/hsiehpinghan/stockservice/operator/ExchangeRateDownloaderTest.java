package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.stockdao.enumeration.CurrencyType;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.springframework.context.ApplicationContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ExchangeRateDownloaderTest {
	private ExchangeRateDownloader downloader;

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		downloader = applicationContext.getBean(ExchangeRateDownloader.class);
	}

	@Test
	public void downloadExchangeRate() {
		List<CurrencyType> dollars = new ArrayList<CurrencyType>(1);
		dollars.add(CurrencyType.USD);
		File f = downloader.downloadExchangeRate(dollars);
		Assert.assertTrue(f.list().length > 0);
	}
}

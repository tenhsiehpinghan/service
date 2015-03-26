package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.hbaseassistant.utility.HbaseEntityTestUtility;
import idv.hsiehpinghan.stockdao.repository.StockClosingConditionRepository;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StockClosingConditionHbaseManagerTest {
	private StockClosingConditionHbaseManager manager;
	private StockServiceProperty stockServiceProperty;
	private StockClosingConditionRepository conditionRepo;

	@BeforeClass
	public void beforeClass() throws Exception {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		stockServiceProperty = applicationContext
				.getBean(StockServiceProperty.class);
		manager = applicationContext
				.getBean(StockClosingConditionHbaseManager.class);
		conditionRepo = applicationContext
				.getBean(StockClosingConditionRepository.class);

		// dropAndCreateTable();
	}

	@Test
	public void saveStockClosingConditionOfTwseToHBase() throws Exception {
		File dir = stockServiceProperty
				.getStockClosingConditionDownloadDirOfTwse();
		// truncateProcessedLogFle(dir);
		int processedAmt = manager.saveStockClosingConditionOfTwseToHBase(dir);
		Assert.assertTrue(processedAmt > 0);
	}

	@Test
	public void saveStockClosingConditionOfGretaiToHBase() throws Exception {
		File dir = stockServiceProperty
				.getStockClosingConditionDownloadDirOfGretai();
		// truncateProcessedLogFle(dir);
		int processedAmt = manager
				.saveStockClosingConditionOfGretaiToHBase(dir);
		Assert.assertTrue(processedAmt > 0);
	}

	private void truncateProcessedLogFle(File dir) throws IOException {
		File processedLog = new File(dir, "processed.log");
		FileUtils.write(processedLog, "", false);
	}

	private void dropAndCreateTable() throws Exception {
		HbaseEntityTestUtility.dropAndCreateTargetTable(conditionRepo);
	}
}

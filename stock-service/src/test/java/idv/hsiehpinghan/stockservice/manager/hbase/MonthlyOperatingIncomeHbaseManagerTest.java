package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.hbaseassistant.utility.HbaseEntityTestUtility;
import idv.hsiehpinghan.stockdao.repository.MonthlyOperatingIncomeRepository;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MonthlyOperatingIncomeHbaseManagerTest {
	private MonthlyOperatingIncomeHbaseManager manager;
	private StockServiceProperty stockServiceProperty;
	private MonthlyOperatingIncomeRepository incomeRepo;
	private Date ver = Calendar.getInstance().getTime();

	@BeforeClass
	public void beforeClass() throws Exception {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		stockServiceProperty = applicationContext
				.getBean(StockServiceProperty.class);
		manager = applicationContext
				.getBean(MonthlyOperatingIncomeHbaseManager.class);
		incomeRepo = applicationContext.getBean(MonthlyOperatingIncomeRepository.class);

		// dropAndCreateTable();
	}

	@Test
	public void saveMonthlyOperatingIncomeToHBase() throws Exception {
		File dir = stockServiceProperty.getMonthlyOperatingIncomeDownloadDir();
//		truncateProcessedLogFle(dir);
		manager.saveMonthlyOperatingIncomeToHBase(ver, dir);
		Assert.assertTrue(incomeRepo.getRowAmount() > 0);
	}

//	private void truncateProcessedLogFle(File dir) throws IOException {
//		File processedLog = new File(dir, "processed.log");
//		FileUtils.write(processedLog, "", false);
//	}

	private void dropAndCreateTable() throws Exception {
		HbaseEntityTestUtility.dropAndCreateTargetTable(incomeRepo);
	}
}

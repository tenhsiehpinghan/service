package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.hbaseassistant.utility.HbaseEntityTestUtility;
import idv.hsiehpinghan.stockdao.repository.MainRatioAnalysisRepository;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StatisticAnalysisHbaseManagerTest {
	private StatisticAnalysisHbaseManager manager;
	private MainRatioAnalysisRepository diffRepo;

	@BeforeClass
	public void beforeClass() throws Exception {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		manager = applicationContext
				.getBean(StatisticAnalysisHbaseManager.class);
		diffRepo = applicationContext
				.getBean(MainRatioAnalysisRepository.class);
		// dropAndCreateTable();
	}

	@Test
	public void updateAnalyzedData() throws Exception {
		boolean result = manager.updateAnalyzedData();
		Assert.assertTrue(result);
	}

	private void dropAndCreateTable() throws Exception {
		HbaseEntityTestUtility.dropAndCreateTargetTable(diffRepo);
	}
}

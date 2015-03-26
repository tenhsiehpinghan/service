package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.hbaseassistant.utility.HbaseEntityTestUtility;
import idv.hsiehpinghan.stockdao.entity.Taxonomy;
import idv.hsiehpinghan.stockdao.entity.Xbrl;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InstanceFamily.InstanceValue;
import idv.hsiehpinghan.stockdao.enumeration.PeriodType;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.enumeration.UnitType;
import idv.hsiehpinghan.stockdao.repository.TaxonomyRepository;
import idv.hsiehpinghan.stockdao.repository.XbrlRepository;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;
import idv.hsiehpinghan.testutility.utility.SystemResourceUtility;
import idv.hsiehpinghan.xbrlassistant.enumeration.XbrlTaxonomyVersion;

import java.io.File;
import java.util.Date;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.context.ApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FinancialReportHbaseManagerTest {
	private final String DATE_PATTERN = "yyyyMMdd";
	private FinancialReportHbaseManager manager;
	private TaxonomyRepository taxonomyRepo;
	private XbrlRepository xbrlRepo;
	private StockServiceProperty stockServiceProperty;

	@BeforeClass
	public void beforeClass() throws Exception {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		stockServiceProperty = applicationContext
				.getBean(StockServiceProperty.class);
		manager = applicationContext.getBean(FinancialReportHbaseManager.class);
		taxonomyRepo = applicationContext.getBean(TaxonomyRepository.class);
		xbrlRepo = applicationContext.getBean(XbrlRepository.class);
		// dropAndCreateTable();
	}

	// @Test
	public void updateTaxonomy() throws Exception {
		String tableName = taxonomyRepo.getTargetTableName();
		if (taxonomyRepo.isTableExists(tableName)) {
			taxonomyRepo.dropTable(tableName);
			taxonomyRepo.createTable(taxonomyRepo.getTargetTableClass());
		}
		XbrlTaxonomyVersion[] versions = XbrlTaxonomyVersion.values();
		for (XbrlTaxonomyVersion taxVer : versions) {
			Assert.assertFalse(taxonomyRepo.exists(taxVer));
		}
		manager.updateTaxonomy();
		for (XbrlTaxonomyVersion ver : versions) {
			Assert.assertTrue(taxonomyRepo.exists(ver));
		}
		Taxonomy taxonomy = taxonomyRepo
				.get(XbrlTaxonomyVersion.TIFRS_BASI_CR_2013_03_31);
		Assert.assertTrue(taxonomy.getNameFamily()
				.getLatestQualifierAndValueAsMap().size() > 0);
	}

//	@Test
	public void processXbrlFiles() throws Exception {
		File instanceFile = SystemResourceUtility
				.getFileResource("xbrl-instance/2013-01-sii-01-C/tifrs-fr0-m1-ci-cr-1101-2013Q1.xml");
		manager.processXbrlFiles(instanceFile, new HashSet<String>(0));
		String[] strArr = instanceFile.getName().split("-");
		String stockCode = strArr[5];
		ReportType reportType = ReportType.getMopsReportType(strArr[4]);
		int year = Integer.valueOf(strArr[6].substring(0, 4));
		int season = Integer.valueOf(strArr[6].substring(5, 6));
		Xbrl entity = xbrlRepo.get(stockCode, reportType, year, season);
		// Test version.
		String version = entity.getInfoFamily().getVersion().name();
		Assert.assertEquals(version, "TIFRS_CI_CR_2013_03_31");
		// Test instance.
		String elementId = "tifrs-SCF_DecreaseIncreaseInFinancialAssetsHeldForTrading";
		PeriodType periodType = PeriodType.DURATION;
		Date startDate = DateUtils.parseDate("20130101", DATE_PATTERN);
		Date endDate = DateUtils.parseDate("20130331", DATE_PATTERN);
		InstanceValue val = entity.getInstanceFamily().getInstanceValue(
				elementId, periodType, null, startDate, endDate);
		Assert.assertEquals(val.getUnitType(), UnitType.TWD);
		Assert.assertEquals(val.getValue().toString(), "-120107000");
	}

//	@Test(dependsOnMethods = { "processXbrlFiles" })
	@Test
	public void saveFinancialReportToHBase() throws Exception {
		File xbrlDir = stockServiceProperty.getFinancialReportExtractDir();
		int actual = manager.saveFinancialReportToHBase(xbrlDir);
		String[] ext = { "xml" };
		int expected = FileUtils.listFiles(xbrlDir, ext, true).size();
		Assert.assertEquals(actual, expected);
	}

//	@Test(dependsOnMethods = { "saveFinancialReportToHBase" })
	public void updateXbrlInstance() throws Exception {
		boolean result = manager.updateXbrlInstance();
		Assert.assertTrue(result);
	}

	private void dropAndCreateTable() throws Exception {
		HbaseEntityTestUtility.dropAndCreateTargetTable(xbrlRepo);
	}
}

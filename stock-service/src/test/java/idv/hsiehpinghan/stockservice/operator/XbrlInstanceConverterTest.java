package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.stockdao.entity.Xbrl;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InfoFamily.InfoQualifier;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InstanceFamily.InstanceValue;
import idv.hsiehpinghan.stockdao.entity.Xbrl.MainRatioFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.RatioFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.RowKey;
import idv.hsiehpinghan.stockdao.enumeration.PeriodType;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.enumeration.UnitType;
import idv.hsiehpinghan.stockservice.manager.hbase.FinancialReportHbaseManager;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;
import idv.hsiehpinghan.testutility.utility.SystemResourceUtility;
import idv.hsiehpinghan.xbrlassistant.assistant.InstanceAssistant;
import idv.hsiehpinghan.xbrlassistant.xbrl.Presentation;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class XbrlInstanceConverterTest {
	private FinancialReportHbaseManager manager;
	private XbrlInstanceConverter converter;
	private InstanceAssistant instanceAssistant;
	private List<String> presentIds;
	private String elementId = "ifrs_BasicEarningsLossPerShare";
	private PeriodType periodType = PeriodType.DURATION;
	private Date instant;
	private Date startDate;
	private Date endDate;
	private UnitType unitType = UnitType.TWD;
	private BigDecimal value = new BigDecimal("0.38");

	@BeforeClass
	public void beforeClass() throws Exception {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		manager = applicationContext.getBean(FinancialReportHbaseManager.class);
		converter = applicationContext.getBean(XbrlInstanceConverter.class);
		instanceAssistant = applicationContext.getBean(InstanceAssistant.class);
		presentIds = new ArrayList<String>(4);
		presentIds.add(Presentation.Id.BalanceSheet);
		presentIds.add(Presentation.Id.StatementOfComprehensiveIncome);
		presentIds.add(Presentation.Id.StatementOfCashFlows);
		presentIds.add(Presentation.Id.StatementOfChangesInEquity);
		instant = DateUtils.parseDate("20130331", "yyyyMMdd");
		startDate = DateUtils.parseDate("20130101", "yyyyMMdd");
		endDate = DateUtils.parseDate("20130331", "yyyyMMdd");
	}

	@Test
	public void convert() throws Exception {
		manager.updateTaxonomy();

		File file = SystemResourceUtility
				.getFileResource("xbrl-instance/2013-01-sii-01-C/tifrs-fr0-m1-ci-cr-1101-2013Q1.xml");
		String[] strArr = file.getName().split("-");
		String stockCode = strArr[5];
		ReportType reportType = ReportType.getMopsReportType(strArr[4]);
		int year = Integer.valueOf(strArr[6].substring(0, 4));
		int season = Integer.valueOf(strArr[6].substring(5, 6));
		ObjectNode objNode = instanceAssistant
				.getInstanceJson(file, presentIds);
		Xbrl xbrl = converter.convert(stockCode, reportType, year, season,
				objNode);
		testRowKey(xbrl, stockCode);
		testInfoFamily(xbrl);
		testInstanceFamily(xbrl);
		testItemFamily(xbrl);
		testMainItemFamily(xbrl);
		testRatioFamily(xbrl);
		testMainRatioFamily(xbrl);
	}

	private void testRowKey(Xbrl xbrl, String stockCode) {
		RowKey rowKey = (RowKey) xbrl.getRowKey();
		Assert.assertEquals(stockCode, rowKey.getStockCode());
	}

	private void testInfoFamily(Xbrl xbrl) {
		Set<InfoQualifier> infoQuals = xbrl.getInfoFamily().getQualifiers();
		Assert.assertTrue(infoQuals.size() == 5);
	}

	private void testInstanceFamily(Xbrl xbrl) {
		InstanceValue instVal = xbrl.getInstanceFamily().getInstanceValue(
				elementId, periodType, null, startDate, endDate);
		Assert.assertEquals(unitType, instVal.getUnitType());
		Assert.assertEquals(value, instVal.getValue());
	}

	private void testItemFamily(Xbrl xbrl) {
		Assert.assertEquals(
				value,
				xbrl.getItemFamily().get(elementId, periodType, null,
						startDate, endDate));
	}

	private void testMainItemFamily(Xbrl xbrl) {
		Assert.assertEquals(
				value,
				xbrl.getMainItemFamily().get(elementId, periodType, null,
						startDate, endDate));
	}

	private void testRatioFamily(Xbrl xbrl) {
		RatioFamily ratioFam = xbrl.getRatioFamily();
		// Balance Sheet
		BigDecimal balanceSheetPercent = ratioFam.getRatio(
				"ifrs_OtherCurrentFinancialAssets", PeriodType.INSTANT,
				instant, null, null);
		Assert.assertEquals(0,
				balanceSheetPercent.compareTo(new BigDecimal("0.32")));
		// Statement Of Comprehensive Income
		BigDecimal statementOfComprehensiveIncomePercent = ratioFam.getRatio(
				"tifrs-bsci-ci_OtherIncomeOthers", PeriodType.DURATION, null,
				startDate, endDate);
		Assert.assertEquals(0, statementOfComprehensiveIncomePercent
				.compareTo(new BigDecimal("0.85")));
		// Statement Of CashFlows
		BigDecimal statementOfCashFlowsPercent = ratioFam.getRatio(
				"tifrs-SCF_DecreaseIncreaseInNotesReceivable",
				PeriodType.DURATION, null, startDate, endDate);
		Assert.assertEquals(0,
				statementOfCashFlowsPercent.compareTo(new BigDecimal("-1.5")));
	}

	private void testMainRatioFamily(Xbrl xbrl) {
		MainRatioFamily mainRatioFam = xbrl.getMainRatioFamily();
		// Balance Sheet
		BigDecimal balanceSheetPercent = mainRatioFam.getRatio(
				"ifrs_OtherCurrentFinancialAssets", PeriodType.INSTANT,
				instant, null, null);
		Assert.assertEquals(0,
				balanceSheetPercent.compareTo(new BigDecimal("0.32")));
		// Statement Of Comprehensive Income
		BigDecimal statementOfComprehensiveIncomePercent = mainRatioFam
				.getRatio("tifrs-bsci-ci_OtherIncomeOthers",
						PeriodType.DURATION, null, startDate, endDate);
		Assert.assertEquals(0, statementOfComprehensiveIncomePercent
				.compareTo(new BigDecimal("0.85")));
		// Statement Of CashFlows
		BigDecimal statementOfCashFlowsPercent = mainRatioFam.getRatio(
				"tifrs-SCF_DecreaseIncreaseInNotesReceivable",
				PeriodType.DURATION, null, startDate, endDate);
		Assert.assertEquals(0,
				statementOfCashFlowsPercent.compareTo(new BigDecimal("-1.5")));
	}
}

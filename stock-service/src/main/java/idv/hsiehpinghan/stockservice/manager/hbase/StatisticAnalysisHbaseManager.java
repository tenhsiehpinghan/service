package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.resourceutility.utility.CsvUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.stockdao.entity.MainRatioAnalysis;
import idv.hsiehpinghan.stockdao.entity.MainRatioAnalysis.TTestFamily;
import idv.hsiehpinghan.stockdao.entity.StockInfo;
import idv.hsiehpinghan.stockdao.entity.Xbrl;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.repository.MainRatioAnalysisRepository;
import idv.hsiehpinghan.stockdao.repository.StockInfoRepository;
import idv.hsiehpinghan.stockdao.repository.XbrlRepository;
import idv.hsiehpinghan.stockservice.manager.IStatisticAnalysisManager;
import idv.hsiehpinghan.stockservice.operator.MainRatioComputer;
import idv.hsiehpinghan.stockservice.operator.StatisticAnalysisMailSender;
import idv.hsiehpinghan.stockservice.operator.XbrlTransporter;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.xbrlassistant.enumeration.XbrlTaxonomyVersion;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.mail.MessagingException;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StatisticAnalysisHbaseManager implements
		IStatisticAnalysisManager, InitializingBean {
	private final String NA = StringUtility.NA_STRING;
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File transportedDir;
	private File transportedLog;
	private File analyzedLog;

	@Autowired
	private XbrlTransporter transporter;
	@Autowired
	private MainRatioComputer computer;
	@Autowired
	private StockServiceProperty stockServiceProperty;
	@Autowired
	private XbrlRepository xbrlRepo;
	@Autowired
	private StockInfoRepository infoRepo;
	@Autowired
	private MainRatioAnalysisRepository analysisRepo;
	@Autowired
	private StatisticAnalysisMailSender mailSender;

	@Override
	public void afterPropertiesSet() throws Exception {
		transportedDir = stockServiceProperty.getTransportDir();
		generateTransportedLog();
		generateAnalyzedLog();
	}

	@Override
	public boolean updateAnalyzedData() throws IOException {
		TreeSet<StockInfo.RowKey> rowKeys = infoRepo.getRowKeys();
		// TreeSet<Xbrl> entities = xbrlRepo.scanWithInfoFamilyOnly();
		Set<String> transportedSet = FileUtility
				.readLinesAsHashSet(transportedLog);
		Set<String> analyzedSet = FileUtility.readLinesAsHashSet(analyzedLog);
		try {
			for (StockInfo.RowKey rowKey : rowKeys) {
				String stockCode = rowKey.getStockCode();
				for (ReportType reportType : ReportType.values()) {
					File targetDirectory = transportXbrl(transportedSet,
							stockCode, reportType);
					if (targetDirectory == null) {
						continue;
					}
					File analyzeFile = analyzeMainRatioAnalysis(analyzedSet,
							stockCode, reportType, targetDirectory);
					if (analyzeFile == null) {
						continue;
					}
					saveMainRatioAnalysisToHBase(analyzeFile);
					writeToAnalyzedFile(stockCode, reportType);
				}
			}
		} catch (Exception e) {
			logger.error("Update analyzed data fail !!!");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public MainRatioAnalysis getMainRatioAnalysis(String stockCode,
			ReportType reportType, int year, int season) {
		try {
			return analysisRepo.get(stockCode, reportType, year, season);
		} catch (Exception e) {
			logger.error("Get ratio difference fail !!!");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void sendMainRatioAnalysisMail() {
		String stockCode = "1256";
		ReportType reportType = ReportType.CONSOLIDATED_STATEMENT;
		try {
			mailSender.sendMainRatioAnalysis(stockCode, reportType);
		} catch (MessagingException e) {
			logger.error("Send main ratio analysis mail fail !!!");
			e.printStackTrace();
		}
	}

	void saveMainRatioAnalysisToHBase(File file) throws Exception {
		CSVParser parser = CsvUtility.getParserAtDataStartRow(file,
				getMainRatioAnalysisTargetTitles(file));
		List<MainRatioAnalysis> entities = new ArrayList<MainRatioAnalysis>();
		Date ver = new Date();
		for (CSVRecord record : parser) {
			if (record.size() <= 1) {
				break;
			}
			String stockCode = getString(record.get(0));
			ReportType reportType = ReportType
					.valueOf(getString(record.get(1)));
			int year = Integer.valueOf(getString(record.get(2)));
			int season = Integer.valueOf(getString(record.get(3)));
			String elementId = getString(record.get(4));
			String chineseName = getString(record.get(5));
			String englishName = getString(record.get(6));
			BigDecimal statistic = getBigDecimal(record.get(7));
			BigDecimal degreeOfFreedom = getBigDecimal(record.get(8));
			BigDecimal confidenceInterval = getBigDecimal(record.get(9));
			BigDecimal sampleMean = getBigDecimal(record.get(10));
			BigDecimal hypothesizedMean = getBigDecimal(record.get(11));
			BigDecimal pValue = getBigDecimal(record.get(12));
			MainRatioAnalysis entity = generateEntity(stockCode, reportType,
					year, season, elementId, ver, chineseName, englishName,
					statistic, degreeOfFreedom, confidenceInterval, sampleMean,
					hypothesizedMean, pValue);
			entities.add(entity);
		}
		analysisRepo.put(entities);
		logger.info(file.getName() + " saved to "
				+ analysisRepo.getTargetTableName() + ".");
	}

	// private TreeSet<String> getStockCodes(StockInfoRepository infoRepo) {
	// TreeSet<StockInfo.RowKey> rowKeys = infoRepo.getRowKeys();
	// TreeSet<String> stockCodes = new TreeSet<String>();
	// for (StockInfo.RowKey rowKey : rowKeys) {
	// stockCodes.add(rowKey.getStockCode());
	// }
	// return stockCodes;
	// }

	private MainRatioAnalysis generateEntity(String stockCode,
			ReportType reportType, int year, int season, String elementId,
			Date ver, String chineseName, String englishName,
			BigDecimal statistic, BigDecimal degreeOfFreedom,
			BigDecimal confidenceInterval, BigDecimal sampleMean,
			BigDecimal hypothesizedMean, BigDecimal pValue) {
		MainRatioAnalysis entity = analysisRepo.generateEntity(stockCode,
				reportType, year, season);
		generateTTestFamilyContent(entity, ver, elementId, chineseName,
				englishName, statistic, degreeOfFreedom, confidenceInterval,
				sampleMean, hypothesizedMean, pValue);
		return entity;
	}

	private void generateTTestFamilyContent(MainRatioAnalysis entity, Date ver,
			String elementId, String chineseName, String englishName,
			BigDecimal statistic, BigDecimal degreeOfFreedom,
			BigDecimal confidenceInterval, BigDecimal sampleMean,
			BigDecimal hypothesizedMean, BigDecimal pValue) {
		TTestFamily fam = entity.getTTestFamily();
		fam.setChineseName(elementId, ver, chineseName);
		fam.setEnglishName(elementId, ver, englishName);
		fam.setStatistic(elementId, ver, statistic);
		fam.setDegreeOfFreedom(elementId, ver, degreeOfFreedom);
		fam.setConfidenceInterval(elementId, ver, confidenceInterval);
		fam.setSampleMean(elementId, ver, sampleMean);
		fam.setHypothesizedMean(elementId, ver, hypothesizedMean);
		fam.setPValue(elementId, ver, pValue);
	}

	private void writeToTransportedFile(String stockCode, ReportType reportType)
			throws IOException {
		String infoLine = generateTransportedInfo(stockCode, reportType)
				+ System.lineSeparator();
		FileUtils.write(transportedLog, infoLine, Charsets.UTF_8, true);
	}

	private void writeToAnalyzedFile(String stockCode, ReportType reportType)
			throws IOException {
		String infoLine = generateAnalyzedInfo(stockCode, reportType)
				+ System.lineSeparator();
		FileUtils.write(analyzedLog, infoLine, Charsets.UTF_8, true);
	}

	private void generateTransportedLog() throws IOException {
		if (transportedLog == null) {
			transportedLog = FileUtility.getOrCreateFile(transportedDir,
					"transported.log");
		}
	}

	private void generateAnalyzedLog() throws IOException {
		if (analyzedLog == null) {
			analyzedLog = FileUtility.getOrCreateFile(transportedDir,
					"analyzed.log");
		}
	}

	private boolean isTransported(Set<String> transportedSet, String stockCode,
			ReportType reportType) throws IOException {
		String transportedInfo = generateTransportedInfo(stockCode, reportType);
		if (transportedSet.contains(transportedInfo)) {
			logger.info(transportedInfo + " processed before.");
			return true;
		}
		return false;
	}

	private boolean isAnalyzed(Set<String> analyzedSet, String stockCode,
			ReportType reportType) throws IOException {
		String analyzedInfo = generateAnalyzedInfo(stockCode, reportType);
		if (analyzedSet.contains(analyzedInfo)) {
			logger.info(analyzedInfo + " analyzed before.");
			return true;
		}
		return false;
	}

	private String generateTransportedInfo(String stockCode,
			ReportType reportType) {
		return String.format("%s_%s", stockCode, reportType);
	}

	private String generateAnalyzedInfo(String stockCode, ReportType reportType) {
		return String.format("%s_%s", stockCode, reportType);
	}

	private File analyzeMainRatioAnalysis(Set<String> analyzedSet,
			String stockCode, ReportType reportType, File targetDirectory)
			throws IOException {
		if (isAnalyzed(analyzedSet, stockCode, reportType)) {
			return null;
		}
		logger.info(String.format("%s / %s begin analyze.", stockCode,
				reportType));
		File resultFile = computer.tTestMainRatio(targetDirectory);
		logger.info(String.format("%s / %s finish analyze.", stockCode,
				reportType));

		return resultFile;
	}

	private File transportXbrl(Set<String> transportedSet, String stockCode,
			ReportType reportType) throws IOException, IllegalAccessException,
			NoSuchMethodException, SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException {
		File targetDirectory = FileUtility.getOrCreateDirectory(
				stockServiceProperty.getTransportDir(), stockCode,
				reportType.name());
		if (isTransported(transportedSet, stockCode, reportType)) {
			return targetDirectory;
		}
		logger.info(String.format("%s %s begin transport.", stockCode,
				reportType));
		// False means no data in hbase.
		boolean transRst = transporter.saveHbaseDataToFile(stockCode,
				reportType, targetDirectory);
		logger.info(String.format("%s  %s finish transport.", stockCode,
				reportType));
		if (transRst == false) {
			return null;
		}
		writeToTransportedFile(stockCode, reportType);
		return targetDirectory;
	}

	private String[] getMainRatioAnalysisTargetTitles(File file) {
		return new String[] { "stockCode", "reportType", "year", "season",
				"elementId", "chineseName", "englishName", "statistic",
				"degreeOfFreedom", "confidenceInterval", "sampleMean",
				"hypothesizedMean", "pValue" };
	}

	private String getString(String str) {
		if (NA.equals(str)) {
			return null;
		}
		return str;
	}

	private BigDecimal getBigDecimal(String str) {
		return new BigDecimal(str);
	}

}

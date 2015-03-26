package idv.hsiehpinghan.stockservice.manager.hbase;

import idv.hsiehpinghan.datatypeutility.utility.CharsetUtility;
import idv.hsiehpinghan.resourceutility.utility.CsvUtility;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.stockdao.entity.StockInfo;
import idv.hsiehpinghan.stockdao.entity.StockInfo.CompanyFamily;
import idv.hsiehpinghan.stockdao.enumeration.IndustryType;
import idv.hsiehpinghan.stockdao.enumeration.MarketType;
import idv.hsiehpinghan.stockdao.repository.StockInfoRepository;
import idv.hsiehpinghan.stockservice.manager.ICompanyBasicInfoManager;
import idv.hsiehpinghan.stockservice.operator.CompanyBasicInfoDownloader;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class CompanyBasicInfoHbaseManager implements ICompanyBasicInfoManager,
		InitializingBean {
	private final String[] EXTENSIONS = { "csv" };
	private final Charset BIG5 = CharsetUtility.BIG5;
	private final CSVFormat EXCEL = CSVFormat.EXCEL;
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File downloadDir;
	private File processedLog;

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private StockServiceProperty stockServiceProperty;
	// @Autowired
	// private CompanyBasicInfoDownloader downloader;

	@Autowired
	private StockInfoRepository stockInfoRepo;

	@Override
	public void afterPropertiesSet() throws Exception {
		downloadDir = stockServiceProperty.getCompanyBasicInfoDownloadDir();
		generateProcessedLog();
	}

	@Override
	public StockInfo getStockInfo(String stockCode) {
		try {
			return stockInfoRepo.get(stockCode);
		} catch (Exception e) {
			logger.error("Get stock info(" + stockCode + ") fail !!!");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public synchronized boolean updateCompanyBasicInfo() {
		File dir = downloadCompanyBasicInfo();
		if (dir == null) {
			return false;
		}
		try {
			int processFilesAmt = saveCompanyBasicInfoToHBase(dir);
			logger.info("Saved " + processFilesAmt + " files to "
					+ stockInfoRepo.getTargetTableName() + ".");
			// truncateProcessedLog();
		} catch (Exception e) {
			logger.error("Update company basic info fail !!!");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	int saveCompanyBasicInfoToHBase(File dir) throws IOException,
			NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException, InstantiationException {
		int count = 0;
		Date ver = new Date();
		Set<String> processedSet = FileUtility.readLinesAsHashSet(processedLog);
		// ex. otc_06.csv
		for (File file : FileUtils.listFiles(dir, EXTENSIONS, true)) {
			if (isProcessed(processedSet, file)) {
				continue;
			}
			CSVParser parser = CsvUtility.getParserAtDataStartRow(file, BIG5,
					EXCEL, getTargetTitles(file));
			List<StockInfo> entities = new ArrayList<StockInfo>();
			String[] fnStrArr = file.getName().split("[_.]");
			MarketType marketType = MarketType.getMopsMarketType(fnStrArr[0]);
			IndustryType industryType = IndustryType
					.getMopsIndustryType(getString(fnStrArr[1]));
			for (CSVRecord record : parser) {
				if (record.size() <= 1) {
					break;
				}
				String stockCode = getString(record.get(0));
				String chineseName = getString(record.get(1));
				String englishBriefName = getString(record.get(24));
				String unifiedBusinessNumber = getString(record.get(3));
				String establishmentDate = getString(record.get(10));
				String listingDate = getString(record.get(11));
				String chairman = getString(record.get(4));
				String generalManager = getString(record.get(5));
				String spokesperson = getString(record.get(6));
				String jobTitleOfSpokesperson = getString(record.get(7));
				String actingSpokesman = getString(record.get(8));
				String chineseAddress = getString(record.get(2));
				String telephone = getString(record.get(9));
				String stockTransferAgency = getString(record.get(18));
				String telephoneOfStockTransferAgency = getString(record
						.get(19));
				String addressOfStockTransferAgency = getString(record.get(20));
				String englishAddress = getString(record.get(25));
				String faxNumber = getString(record.get(26));
				String email = getString(record.get(27));
				String webSite = getString(record.get(28));
				String financialReportType = getString(record.get(17));
				String parValueOfOrdinaryShares = getString(record.get(12));
				String paidInCapital = getString(record.get(13));
				String amountOfOrdinaryShares = getString(record.get(14));
				String privatePlacementAmountOfOrdinaryShares = getString(record
						.get(15));
				String amountOfPreferredShares = getString(record.get(16));
				String accountingFirm = getString(record.get(21));
				String accountant1 = getString(record.get(22));
				String accountant2 = getString(record.get(23));
				StockInfo entity = generateEntity(stockCode, ver, marketType,
						industryType, chineseName, englishBriefName,
						unifiedBusinessNumber, establishmentDate, listingDate,
						chairman, generalManager, spokesperson,
						jobTitleOfSpokesperson, actingSpokesman,
						chineseAddress, telephone, stockTransferAgency,
						telephoneOfStockTransferAgency,
						addressOfStockTransferAgency, englishAddress,
						faxNumber, email, webSite, financialReportType,
						parValueOfOrdinaryShares, paidInCapital,
						amountOfOrdinaryShares,
						privatePlacementAmountOfOrdinaryShares,
						amountOfPreferredShares, accountingFirm, accountant1,
						accountant2);
				entities.add(entity);
			}
			stockInfoRepo.put(entities);
			logger.info(file.getName() + " saved to "
					+ stockInfoRepo.getTargetTableName() + ".");
			writeToProcessedFile(file);
			++count;
		}
		return count;
	}

	void truncateProcessedLog() throws IOException {
		FileUtils.write(processedLog, "", Charsets.UTF_8);
	}

	private String getString(String str) {
		return str.trim();
	}

	private boolean isProcessed(Set<String> processedSet, File file)
			throws IOException {
		String processedInfo = generateProcessedInfo(file);
		if (processedSet.contains(processedInfo)) {
			logger.info(processedInfo + " processed before.");
			return true;
		}
		return false;
	}

	private String generateProcessedInfo(File file) {
		return file.getName();
	}

	private void writeToProcessedFile(File file) throws IOException {
		String infoLine = generateProcessedInfo(file) + System.lineSeparator();
		FileUtils.write(processedLog, infoLine, Charsets.UTF_8, true);
	}

	private StockInfo generateEntity(String stockCode, Date ver,
			MarketType marketType, IndustryType industryType,
			String chineseName, String englishBriefName,
			String unifiedBusinessNumber, String establishmentDate,
			String listingDate, String chairman, String generalManager,
			String spokesperson, String jobTitleOfSpokesperson,
			String actingSpokesman, String chineseAddress, String telephone,
			String stockTransferAgency, String telephoneOfStockTransferAgency,
			String addressOfStockTransferAgency, String englishAddress,
			String faxNumber, String email, String webSite,
			String financialReportType, String parValueOfOrdinaryShares,
			String paidInCapital, String amountOfOrdinaryShares,
			String privatePlacementAmountOfOrdinaryShares,
			String amountOfPreferredShares, String accountingFirm,
			String accountant1, String accountant2) {
		StockInfo entity = new StockInfo();
		entity.new RowKey(stockCode, entity);
		generateCompanyFamilyContent(entity, ver, marketType, industryType,
				chineseName, englishBriefName, unifiedBusinessNumber,
				establishmentDate, listingDate, chairman, generalManager,
				spokesperson, jobTitleOfSpokesperson, actingSpokesman,
				chineseAddress, telephone, stockTransferAgency,
				telephoneOfStockTransferAgency, addressOfStockTransferAgency,
				englishAddress, faxNumber, email, webSite, financialReportType,
				parValueOfOrdinaryShares, paidInCapital,
				amountOfOrdinaryShares, privatePlacementAmountOfOrdinaryShares,
				amountOfPreferredShares, accountingFirm, accountant1,
				accountant2);
		return entity;
	}

	private void generateCompanyFamilyContent(StockInfo entity, Date ver,
			MarketType marketType, IndustryType industryType,
			String chineseName, String englishBriefName,
			String unifiedBusinessNumber, String establishmentDate,
			String listingDate, String chairman, String generalManager,
			String spokesperson, String jobTitleOfSpokesperson,
			String actingSpokesman, String chineseAddress, String telephone,
			String stockTransferAgency, String telephoneOfStockTransferAgency,
			String addressOfStockTransferAgency, String englishAddress,
			String faxNumber, String email, String webSite,
			String financialReportType, String parValueOfOrdinaryShares,
			String paidInCapital, String amountOfOrdinaryShares,
			String privatePlacementAmountOfOrdinaryShares,
			String amountOfPreferredShares, String accountingFirm,
			String accountant1, String accountant2) {
		CompanyFamily fam = entity.getCompanyFamily();
		fam.setAccountant1(ver, accountant1);
		fam.setAccountant2(ver, accountant2);
		fam.setAccountingFirm(ver, accountingFirm);
		fam.setActingSpokesman(ver, actingSpokesman);
		fam.setAddressOfStockTransferAgency(ver, addressOfStockTransferAgency);
		fam.setAmountOfOrdinaryShares(ver, amountOfOrdinaryShares);
		fam.setAmountOfPreferredShares(ver, amountOfPreferredShares);
		fam.setChairman(ver, chairman);
		fam.setChineseAddress(ver, chineseAddress);
		fam.setChineseName(ver, chineseName);
		fam.setEmail(ver, email);
		fam.setEnglishAddress(ver, englishAddress);
		fam.setEnglishBriefName(ver, englishBriefName);
		fam.setEstablishmentDate(ver, establishmentDate);
		fam.setFaxNumber(ver, faxNumber);
		fam.setFinancialReportType(ver, financialReportType);
		fam.setGeneralManager(ver, generalManager);
		fam.setIndustryType(ver, industryType);
		fam.setJobTitleOfSpokesperson(ver, jobTitleOfSpokesperson);
		fam.setListingDate(ver, listingDate);
		fam.setMarketType(ver, marketType);
		fam.setPaidInCapital(ver, paidInCapital);
		fam.setParValueOfOrdinaryShares(ver, parValueOfOrdinaryShares);
		fam.setPrivatePlacementAmountOfOrdinaryShares(ver,
				privatePlacementAmountOfOrdinaryShares);
		fam.setSpokesperson(ver, spokesperson);
		fam.setStockTransferAgency(ver, stockTransferAgency);
		fam.setTelephone(ver, telephone);
		fam.setTelephoneOfStockTransferAgency(ver,
				telephoneOfStockTransferAgency);
		fam.setUnifiedBusinessNumber(ver, unifiedBusinessNumber);
		fam.setWebSite(ver, webSite);
	}

	private File downloadCompanyBasicInfo() {
		CompanyBasicInfoDownloader downloader = applicationContext
				.getBean(CompanyBasicInfoDownloader.class);
		try {
			File dir = downloader.downloadCompanyBasicInfo();
			logger.info(dir.getAbsolutePath() + " download finish.");
			return dir;
		} catch (Exception e) {
			logger.error("Download company basic info fail !!!");
			return null;
		}
	}

	private void generateProcessedLog() throws IOException {
		if (processedLog == null) {
			processedLog = new File(downloadDir, "processed.log");
			if (processedLog.exists() == false) {
				FileUtils.touch(processedLog);
			}
		}
	}

	private String[] getTargetTitles(File file) {
		String fileName = file.getName();
		if (fileName.startsWith("sii")) {
			return new String[] { "公司代號", "公司名稱", "住址", "營利事業統一編號", "董事長",
					"總經理", "發言人", "發言人職稱", "代理發言人", "總機電話", "成立日期", "上市日期",
					"普通股每股面額", "實收資本額(元)", "已發行普通股數或TDR原發行股數", "私募普通股(股)",
					"特別股(股)", "編製財務報告類型", "股票過戶機構", "過戶電話", "過戶地址", "簽證會計師事務所",
					"簽證會計師1", "簽證會計師2", "英文簡稱", "英文通訊地址", "傳真機號碼", "電子郵件信箱",
					"網址" };
		} else if (fileName.startsWith("otc")) {
			return new String[] { "公司代號", "公司名稱", "住址", "營利事業統一編號", "董事長",
					"總經理", "發言人", "發言人職稱", "代理發言人", "總機電話", "成立日期", "上櫃日期",
					"普通股每股面額", "實收資本額(元)", "已發行普通股數或TDR原發行股數", "私募普通股(股)",
					"特別股(股)", "編製財務報告類型", "股票過戶機構", "過戶電話", "過戶地址", "簽證會計師事務所",
					"簽證會計師1", "簽證會計師2", "英文簡稱", "英文通訊地址", "傳真機號碼", "電子郵件信箱",
					"網址" };
		} else {
			throw new RuntimeException("Filename(" + file.getAbsolutePath()
					+ ") unexpected.");
		}
	}
}

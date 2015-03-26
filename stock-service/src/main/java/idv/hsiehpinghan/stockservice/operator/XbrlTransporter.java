package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datatypeutility.utility.CharsetUtility;
import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.datetimeutility.utility.DateUtility;
import idv.hsiehpinghan.hbaseassistant.abstractclass.HBaseColumnQualifier;
import idv.hsiehpinghan.hbaseassistant.abstractclass.HBaseValue;
import idv.hsiehpinghan.resourceutility.utility.FileUtility;
import idv.hsiehpinghan.stockdao.entity.Taxonomy.NameFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl;
import idv.hsiehpinghan.stockdao.entity.Xbrl.MainRatioFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.MainRatioFamily.MainRatioQualifier;
import idv.hsiehpinghan.stockdao.entity.Xbrl.MainRatioFamily.MainRatioValue;
import idv.hsiehpinghan.stockdao.entity.Xbrl.RowKey;
import idv.hsiehpinghan.stockdao.enumeration.PeriodType;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.repository.TaxonomyRepository;
import idv.hsiehpinghan.stockdao.repository.XbrlRepository;
import idv.hsiehpinghan.xbrlassistant.enumeration.XbrlTaxonomyVersion;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class XbrlTransporter {
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private final String YYYY_MM_DD = "yyyy-MM-dd";
	private final Charset UTF_8 = CharsetUtility.UTF_8;
	private final String NA = StringUtility.NA_STRING;
	private final String XBRL = "xbrl";

	@Autowired
	private XbrlRepository xbrlRepo;
	@Autowired
	private TaxonomyRepository taxonomyRepo;

	public boolean saveHbaseDataToFile(String stockCode, ReportType reportType,
			File targetDirectory) throws IOException, IllegalAccessException,
			NoSuchMethodException, SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException {
		TreeSet<Xbrl> entities = xbrlRepo.fuzzyScan(stockCode, reportType,
				null, null);
		if (entities.size() <= 0) {
			logger.info(String
					.format("XbrlRepository fuzzyScan get nothing.[StockCode(%s), ReportType(%s)]",
							stockCode, reportType));
			return false;
		}
		File targetFile = FileUtility.getOrCreateFile(targetDirectory, XBRL);
		FileUtils.write(targetFile, generateTitle(), UTF_8, false);
		XbrlTaxonomyVersion version = getXbrlTaxonomyVersion(entities);
		NameFamily nameFam = taxonomyRepo.get(version).getNameFamily();
		for (Xbrl entity : entities) {
			writeToFile(targetFile, entity, nameFam);
		}
		return true;
	}

	private XbrlTaxonomyVersion getXbrlTaxonomyVersion(TreeSet<Xbrl> entities) {
		return entities.last().getInfoFamily().getVersion();
	}

	private void writeToFile(File targetFile, Xbrl entity, NameFamily nameFam)
			throws IOException {
		RowKey rowKey = (RowKey) entity.getRowKey();
		String stockCode = rowKey.getStockCode();
		ReportType reportType = rowKey.getReportType();
		int year = rowKey.getYear();
		int season = rowKey.getSeason();
		MainRatioFamily mainRatioFam = entity.getMainRatioFamily();
		for (Entry<HBaseColumnQualifier, HBaseValue> ent : mainRatioFam
				.getLatestQualifierAndValueAsSet()) {
			MainRatioQualifier qual = (MainRatioQualifier) ent.getKey();
			String elementId = qual.getElementId();
			PeriodType periodType = qual.getPeriodType();
			Date instant = qual.getInstant();
			Date startDate = qual.getStartDate();
			Date endDate = qual.getEndDate();
			String chineseName = nameFam.getChineseName(elementId);
			String englishName = nameFam.getEnglishName(elementId);
			MainRatioValue val = (MainRatioValue) ent.getValue();
			BigDecimal ratio = val.getAsBigDecimal();
			String record = generateRecord(stockCode, reportType, year, season,
					elementId, periodType, instant, startDate, endDate,
					chineseName, englishName, ratio);
			FileUtils.write(targetFile, record, UTF_8, true);
		}
	}

	private String generateTitle() {
		return "stockCode\treportType\tyear\tseason\telementId\tperiodType\tinstant\tstartDate\tendDate\tchineseName\tenglishName\tratio"
				+ System.lineSeparator();
	}

	private String generateRecord(String stockCode, ReportType reportType,
			int year, int season, String elementId, PeriodType periodType,
			Date instant, Date startDate, Date endDate, String chineseName,
			String englishName, BigDecimal ratio) {
		String instantStr = DateUtility.getDateString(instant, YYYY_MM_DD, NA);
		String startDateStr = DateUtility.getDateString(startDate, YYYY_MM_DD,
				NA);
		String endDateStr = DateUtility.getDateString(endDate, YYYY_MM_DD, NA);
		return String.format(
				"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%s", stockCode,
				reportType, year, season, elementId, periodType, instantStr,
				startDateStr, endDateStr, chineseName, englishName, ratio,
				System.lineSeparator());
	}

}

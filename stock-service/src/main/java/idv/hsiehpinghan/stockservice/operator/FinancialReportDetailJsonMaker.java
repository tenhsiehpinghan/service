package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.stockdao.entity.Taxonomy.PresentationFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InfoFamily;
import idv.hsiehpinghan.stockdao.enumeration.PeriodType;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.repository.TaxonomyRepository;
import idv.hsiehpinghan.stockdao.repository.XbrlRepository;
import idv.hsiehpinghan.xbrlassistant.xbrl.Presentation;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Service
public class FinancialReportDetailJsonMaker {
	// private Logger logger = Logger.getLogger(this.getClass().getName());
	public static final String INFO = "info";
	public static final String STOCK_CODE = "stockCode";
	public static final String REPORT_TYPE = "reportType";
	public static final String YEAR = "year";
	public static final String SEASON = "season";
	public static final String LOCALE = "locale";
	public static final String CHINESE_LABEL = "chinese_label";
	public static final String ENGLISH_LABEL = "english_label";
	private static final String COMMA_STRING = StringUtility.COMMA_STRING;
	private static final String YYYYMMDD = "yyyyMMdd";
	private static final String YYYY_MM_DD = "yyyy/MM/dd";
	private static final String DEEP = "deep";
	private static final String LABEL = "label";
	private static final String TITLE = "title";

	@Autowired
	private ObjectMapper objectMapper;
	@Autowired
	private XbrlRepository xbrlRepo;
	@Autowired
	private TaxonomyRepository taxoRepo;

	public Map<String, ObjectNode> getPresentationJsonMap(
			List<String> presentIds, String stockCode, ReportType reportType,
			Integer year, Integer season, Locale locale)
			throws IllegalAccessException, NoSuchMethodException,
			SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException, IOException,
			NoSuchFieldException, ParseException {
		if (xbrlRepo.exists(stockCode, reportType, year, season) == false) {
			return null;
		}
		Xbrl xbrl = xbrlRepo.get(stockCode, reportType, year, season);
		InfoFamily infoFam = xbrl.getInfoFamily();
		PresentationFamily presentFamily = taxoRepo.get(infoFam.getVersion())
				.getPresentationFamily();
		Map<String, ObjectNode> map = new HashMap<String, ObjectNode>(
				presentIds.size());
		map.put(INFO,
				generateInfoJsonObject(stockCode, reportType, year, season,
						locale));
		for (String presentId : presentIds) {
			ObjectNode objNode = null;
			if (Presentation.Id.BalanceSheet.equals(presentId)) {
				String jsonStr = presentFamily.getBalanceSheet();
				String[] periods = infoFam.getBalanceSheetContext().split(
						COMMA_STRING);
				objNode = generateJsonObject(locale, presentId, jsonStr,
						PeriodType.INSTANT, periods, xbrl);
			} else if (Presentation.Id.StatementOfComprehensiveIncome
					.equals(presentId)) {
				String jsonStr = presentFamily
						.getStatementOfComprehensiveIncome();
				String[] periods = infoFam
						.getStatementOfComprehensiveIncomeContext().split(
								COMMA_STRING);
				objNode = generateJsonObject(locale, presentId, jsonStr,
						PeriodType.DURATION, periods, xbrl);
			} else if (Presentation.Id.StatementOfCashFlows.equals(presentId)) {
				String jsonStr = presentFamily.getStatementOfCashFlows();
				String[] periods = infoFam.getStatementOfCashFlowsContext()
						.split(COMMA_STRING);
				objNode = generateJsonObject(locale, presentId, jsonStr,
						PeriodType.DURATION, periods, xbrl);
			} else if (Presentation.Id.StatementOfChangesInEquity
					.equals(presentId)) {
				String jsonStr = presentFamily.getStatementOfChangesInEquity();
				String[] periods = infoFam
						.getStatementOfChangesInEquityContext().split(
								COMMA_STRING);
				objNode = generateJsonObject(locale, presentId, jsonStr,
						PeriodType.DURATION, periods, xbrl);
			} else {
				throw new RuntimeException("Presentation id(" + presentId
						+ ") not implements !!!");
			}
			map.put(presentId, objNode);
		}
		return map;
	}

	private ObjectNode generateInfoJsonObject(String stockCode,
			ReportType reportType, Integer year, Integer season, Locale locale) {
		ObjectNode infoNode = objectMapper.createObjectNode();
		infoNode.put(STOCK_CODE, stockCode);
		infoNode.put(REPORT_TYPE, reportType.name());
		infoNode.put(YEAR, year);
		infoNode.put(SEASON, season);
		infoNode.put(LOCALE, locale.getLanguage());
		return infoNode;
	}

	private ObjectNode generateJsonObject(Locale locale, String presentId,
			String jsonString, PeriodType periodType, String[] periods,
			Xbrl xbrl) throws JsonProcessingException, IOException,
			ParseException {
		ObjectNode srcNode = (ObjectNode) objectMapper.readTree(jsonString);
		ObjectNode targetNode = objectMapper.createObjectNode();
		generateTitleNode(targetNode, periodType, periods);
		generateContentNodes(locale, srcNode, targetNode, presentId,
				periodType, periods, xbrl);
		return targetNode;
	}

	private void generateTitleNode(ObjectNode targetNode,
			PeriodType periodType, String[] periods) throws ParseException {
		ObjectNode titleNode = objectMapper.createObjectNode();
		if (PeriodType.INSTANT.equals(periodType)) {
			for (String period : periods) {
				titleNode.put(period, getInstantPeriodTitle(period));
			}
		} else if (PeriodType.DURATION.equals(periodType)) {
			for (String period : periods) {
				titleNode.put(period, getDurationPeriodTitle(period));
			}
		} else {
			throw new RuntimeException("PeriodType(" + periodType
					+ ") not implements !!!");
		}

		targetNode.set(TITLE, titleNode);
	}

	private String getInstantPeriodTitle(String period) throws ParseException {
		Date date = getInstantDate(period);
		return DateFormatUtils.format(date, YYYY_MM_DD);
	}

	private String getDurationPeriodTitle(String period) throws ParseException {
		Date startDate = getStartDate(period);
		Date endDate = getEndDate(period);
		return DateFormatUtils.format(startDate, YYYY_MM_DD) + " ~ "
				+ DateFormatUtils.format(endDate, YYYY_MM_DD);
	}

	private void generateContentNodes(Locale locale, ObjectNode srcNode,
			ObjectNode targetNode, String presentId, PeriodType periodType,
			String[] periods, Xbrl xbrl) throws ParseException {
		generateContentNode(locale, srcNode, targetNode, presentId, periodType,
				periods, xbrl, 0);
	}

	private boolean generateContentNode(Locale locale, ObjectNode srcNode,
			ObjectNode targetNode, String presentId, PeriodType periodType,
			String[] periods, Xbrl xbrl, int deep) throws ParseException {
		boolean hasContent = false;
		Iterator<Map.Entry<String, JsonNode>> iter = srcNode.fields();
		while (iter.hasNext()) {
			// Reset hasContent.
			hasContent = false;
			Map.Entry<String, JsonNode> ent = iter.next();
			String key = ent.getKey();
			JsonNode node = ent.getValue();
			if (node.isObject()) {
				ObjectNode objNode = objectMapper.createObjectNode();
				targetNode.set(key, objNode);
				objNode.put(DEEP, deep);
				if (Locale.ENGLISH.getLanguage().equals(locale.getLanguage())) {
					objNode.put(LABEL, node.get(ENGLISH_LABEL).asText());
				} else {
					objNode.put(LABEL, node.get(CHINESE_LABEL).asText());
				}
				if (PeriodType.INSTANT.equals(periodType)) {
					for (String period : periods) {
						BigDecimal itemValue = getInstantItemValue(key, period,
								xbrl);
						if (itemValue == null) {
							continue;
						}
						objNode.put(period, itemValue);
						hasContent |= true;
					}
				} else if (PeriodType.DURATION.equals(periodType)) {
					for (String period : periods) {
						BigDecimal itemValue = getDurationItemValue(key,
								period, xbrl);
						if (itemValue == null) {
							continue;
						}
						objNode.put(period, itemValue);
						hasContent |= true;
					}
				} else {
					throw new RuntimeException("Period type(" + periodType
							+ ") not implement !!!");
				}
				hasContent |= generateContentNode(locale, (ObjectNode) node,
						targetNode, presentId, periodType, periods, xbrl,
						deep + 1);
				if (hasContent == false) {
					targetNode.remove(key);
				}
			}
		}
		return hasContent;
	}

	private BigDecimal getInstantItemValue(String elementId, String period,
			Xbrl xbrl) throws ParseException {
		Date instant = DateUtils.parseDate(period, YYYYMMDD);
		return xbrl.getItemFamily().get(elementId, PeriodType.INSTANT, instant,
				null, null);
	}

	private BigDecimal getDurationItemValue(String elementId, String period,
			Xbrl xbrl) throws ParseException {
		Date startDate = getStartDate(period);
		Date endDate = getEndDate(period);
		return xbrl.getItemFamily().get(elementId, PeriodType.DURATION, null,
				startDate, endDate);
	}

	private Date getInstantDate(String period) throws ParseException {
		return DateUtils.parseDate(period, YYYYMMDD);
	}

	private Date getStartDate(String period) throws ParseException {
		String[] dates = period.split("~");
		return DateUtils.parseDate(dates[0], YYYYMMDD);
	}

	private Date getEndDate(String period) throws ParseException {
		String[] dates = period.split("~");
		return DateUtils.parseDate(dates[1], YYYYMMDD);
	}
}

package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.datatypeutility.utility.BigDecimalUtility;
import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.datetimeutility.utility.DateUtility;
import idv.hsiehpinghan.hbaseassistant.abstractclass.HBaseColumnQualifier;
import idv.hsiehpinghan.hbaseassistant.abstractclass.HBaseValue;
import idv.hsiehpinghan.stockdao.entity.Taxonomy.PresentationFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InfoFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InstanceFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InstanceFamily.InstanceQualifier;
import idv.hsiehpinghan.stockdao.entity.Xbrl.InstanceFamily.InstanceValue;
import idv.hsiehpinghan.stockdao.entity.Xbrl.ItemFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.ItemFamily.ItemQualifier;
import idv.hsiehpinghan.stockdao.entity.Xbrl.ItemFamily.ItemValue;
import idv.hsiehpinghan.stockdao.entity.Xbrl.MainItemFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.MainRatioFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.RatioFamily;
import idv.hsiehpinghan.stockdao.entity.Xbrl.RatioFamily.RatioQualifier;
import idv.hsiehpinghan.stockdao.entity.Xbrl.RatioFamily.RatioValue;
import idv.hsiehpinghan.stockdao.enumeration.PeriodType;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.enumeration.UnitType;
import idv.hsiehpinghan.stockdao.repository.TaxonomyRepository;
import idv.hsiehpinghan.stockdao.repository.XbrlRepository;
import idv.hsiehpinghan.xbrlassistant.assistant.InstanceAssistant;
import idv.hsiehpinghan.xbrlassistant.enumeration.XbrlTaxonomyVersion;
import idv.hsiehpinghan.xbrlassistant.xbrl.Instance;
import idv.hsiehpinghan.xbrlassistant.xbrl.Presentation;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Convert xbrl json to xbrl instance.
 * 
 * @author thank.hsiehpinghan
 *
 */
@Service
public class XbrlInstanceConverter {
	// private Logger logger = Logger.getLogger(this.getClass().getName());
	private static final String COMMA_STRING = StringUtility.COMMA_STRING;
	private static final String YYYYMMDD = "yyyyMMdd";
	private static final String ABSTRACT = "Abstract";

	@Autowired
	private TaxonomyRepository taxoRepo;
	@Autowired
	private XbrlRepository xbrlRepo;
	@Autowired
	private ObjectMapper objectMapper;

	public Xbrl convert(String stockCode, ReportType reportType, int year,
			int season, ObjectNode objNode) throws ParseException,
			IllegalAccessException, NoSuchMethodException, SecurityException,
			InstantiationException, IllegalArgumentException,
			InvocationTargetException, IOException {
		Xbrl entity = xbrlRepo.generateEntity(stockCode, reportType, year,
				season);
		generateRowKey(entity, stockCode, reportType, year, season);
		generateColumnFamilies(entity, objNode);
		return entity;
	}

	private void generateColumnFamilies(Xbrl entity, ObjectNode objNode)
			throws ParseException, IllegalAccessException,
			NoSuchMethodException, SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException, IOException {
		Date ver = Calendar.getInstance().getTime();
		generateInfoFamily(entity, objNode, ver);
		generateInstanceFamily(entity, objNode, ver);
		generateItemFamily(entity, ver);
		generateMainItemFamily(entity, ver);
		generateRatioFamily(entity, ver);
		generateMainRatioFamily(entity, ver);
	}

	private Date getDate(JsonNode dateNode) throws ParseException {
		if (dateNode == null) {
			return null;
		}
		return DateUtils.parseDate(dateNode.textValue(), YYYYMMDD);
	}

	private BigDecimal getTwdValue(UnitType unitType, BigDecimal value) {
		if (UnitType.TWD.equals(unitType)) {
			return value;
		} else if (UnitType.SHARES.equals(unitType)) {
			return value;
		} else {
			throw new RuntimeException("UnitType(" + unitType
					+ ") not implement !!!");
		}
	}

	private ObjectNode getPresentationJson(Xbrl entity, String presentId)
			throws IllegalAccessException, NoSuchMethodException,
			SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException, IOException {
		InfoFamily infoFam = entity.getInfoFamily();
		PresentationFamily presentFamily = taxoRepo.get(infoFam.getVersion())
				.getPresentationFamily();
		String jsonStr = null;
		switch (presentId) {
		case Presentation.Id.BalanceSheet:
			jsonStr = presentFamily.getBalanceSheet();
			break;
		case Presentation.Id.StatementOfComprehensiveIncome:
			jsonStr = presentFamily.getStatementOfComprehensiveIncome();
			break;
		case Presentation.Id.StatementOfCashFlows:
			jsonStr = presentFamily.getStatementOfCashFlows();
			break;
		case Presentation.Id.StatementOfChangesInEquity:
			jsonStr = presentFamily.getStatementOfChangesInEquity();
			break;
		default:
			throw new RuntimeException("Presentation id(" + presentId
					+ ") not implements !!!");
		}
		return (ObjectNode) objectMapper.readTree(jsonStr);
	}

	private void generateRatioContent(Xbrl entity, Date ver, String presentId,
			PeriodType periodType, String[] periods)
			throws IllegalAccessException, NoSuchMethodException,
			SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException, IOException,
			ParseException {
		ObjectNode presentNode = getPresentationJson(entity, presentId);
		generateRatio(entity, ver, presentId, periodType, periods, presentNode);
	}

	private String getRealKey(String key, ObjectNode node) {
		if (key.endsWith(ABSTRACT) == false) {
			return key;
		}
		String realKey = null;
		Iterator<String> iter = node.fieldNames();
		while (iter.hasNext()) {
			realKey = iter.next();
		}
		return realKey;
	}

	private BigDecimal getInstantTotalValue(Xbrl entity,
			ObjectNode presentNode, String realKey, Date instant)
			throws ParseException {
		if (realKey == null) {
			return null;
		}
		return entity.getItemFamily().get(realKey, PeriodType.INSTANT, instant,
				null, null);
	}

	private BigDecimal getDurationTotalValue(Xbrl entity,
			ObjectNode presentNode, String realKey, Date startDate, Date endDate)
			throws ParseException {
		if (realKey == null) {
			return null;
		}
		return entity.getItemFamily().get(realKey, PeriodType.DURATION, null,
				startDate, endDate);
	}

	private void generateRatio(Xbrl entity, Date ver, String presentId,
			PeriodType periodType, String[] periods, ObjectNode presentNode)
			throws IllegalAccessException, NoSuchMethodException,
			SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException, IOException,
			ParseException {
		Iterator<Map.Entry<String, JsonNode>> iter = presentNode.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> ent = iter.next();
			String key = ent.getKey();
			JsonNode node = ent.getValue();
			if (node.isObject()) {
				if (PeriodType.INSTANT.equals(periodType)) {
					for (String period : periods) {
						Date instant = DateUtils.parseDate(period, YYYYMMDD);
						String realKey = getRealKey(key, (ObjectNode) node);
						BigDecimal totalValue = getInstantTotalValue(entity,
								presentNode, realKey, instant);
						if (totalValue == null) {
							continue;
						}
						setInstantRatio(entity, ver, (ObjectNode) node,
								realKey, totalValue, instant);
					}
				} else if (PeriodType.DURATION.equals(periodType)) {
					for (String period : periods) {
						String[] dates = period.split("~");
						Date startDate = DateUtils
								.parseDate(dates[0], YYYYMMDD);
						Date endDate = DateUtils.parseDate(dates[1], YYYYMMDD);
						String realKey = getRealKey(key, (ObjectNode) node);
						BigDecimal totalValue = getDurationTotalValue(entity,
								presentNode, realKey, startDate, endDate);
						if (totalValue == null) {
							continue;
						}
						setDurationRatio(entity, ver, (ObjectNode) node,
								realKey, totalValue, startDate, endDate);
					}
				} else {
					throw new RuntimeException("Period type(" + periodType
							+ ") not implement !!!");
				}
				generateRatio(entity, ver, presentId, periodType, periods,
						(ObjectNode) node);
			}
		}
	}

	private void setInstantRatio(Xbrl entity, Date ver, ObjectNode objNode,
			String realParentKey, BigDecimal parentValue, Date instant) {
		setRatio(entity, ver, objNode, realParentKey, parentValue,
				PeriodType.INSTANT, instant, null, null);
	}

	private void setDurationRatio(Xbrl entity, Date ver, ObjectNode objNode,
			String realParentKey, BigDecimal parentValue, Date startDate,
			Date endDate) {
		setRatio(entity, ver, objNode, realParentKey, parentValue,
				PeriodType.DURATION, null, startDate, endDate);
	}

	private void setRatio(Xbrl entity, Date ver, ObjectNode objNode,
			String realParentKey, BigDecimal parentValue,
			PeriodType periodType, Date instant, Date startDate, Date endDate) {
		RatioFamily ratioFam = entity.getRatioFamily();
		Iterator<Map.Entry<String, JsonNode>> iter = objNode.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> ent = iter.next();
			String key = ent.getKey();
			JsonNode node = ent.getValue();
			if (node.isObject() == false) {
				continue;
			}
			if (realParentKey.equals(key)) {
				continue;
			}
			String realKey = getRealKey(key, (ObjectNode) node);
			BigDecimal value = null;
			if (PeriodType.INSTANT.equals(periodType)) {
				value = entity.getItemFamily().get(realKey, periodType,
						instant, null, null);
			} else if (PeriodType.DURATION.equals(periodType)) {
				value = entity.getItemFamily().get(realKey, periodType, null,
						startDate, endDate);
			} else {
				throw new RuntimeException("Period type(" + periodType
						+ ") undefined !!!");
			}
			BigDecimal ratio = BigDecimalUtility.divide(value, parentValue);
			if (ratio == null) {
				continue;
			}
			if (PeriodType.INSTANT.equals(periodType)) {
				ratioFam.setRatio(realKey, periodType, instant, null, null,
						ver, ratio);
			} else if (PeriodType.DURATION.equals(periodType)) {
				ratioFam.setRatio(realKey, periodType, null, startDate,
						endDate, ver, ratio);
			} else {
				throw new RuntimeException("Period type(" + periodType
						+ ") undefined !!!");
			}
		}
	}

	private void generateRatioFamily(Xbrl entity, Date ver)
			throws IllegalAccessException, NoSuchMethodException,
			SecurityException, InstantiationException,
			IllegalArgumentException, InvocationTargetException, IOException,
			ParseException {
		String[] balanceSheetPeriods = entity.getInfoFamily()
				.getBalanceSheetContext().split(COMMA_STRING);
		generateRatioContent(entity, ver, Presentation.Id.BalanceSheet,
				PeriodType.INSTANT, balanceSheetPeriods);
		String[] statementOfComprehensiveIncomePeriods = entity.getInfoFamily()
				.getStatementOfComprehensiveIncomeContext().split(COMMA_STRING);
		generateRatioContent(entity, ver,
				Presentation.Id.StatementOfComprehensiveIncome,
				PeriodType.DURATION, statementOfComprehensiveIncomePeriods);
		String[] statementOfCashFlowsPeriods = entity.getInfoFamily()
				.getStatementOfCashFlowsContext().split(COMMA_STRING);
		generateRatioContent(entity, ver, Presentation.Id.StatementOfCashFlows,
				PeriodType.DURATION, statementOfCashFlowsPeriods);
	}

	private void generateMainItemFamily(Xbrl entity, Date ver) {
		ItemFamily itemFam = entity.getItemFamily();
		MainItemFamily mainItemFam = entity.getMainItemFamily();
		String oldElementId = null;
		for (Entry<HBaseColumnQualifier, HBaseValue> qualValEnt : itemFam
				.getLatestQualifierAndValueAsDescendingSet()) {
			ItemQualifier qual = (ItemQualifier) qualValEnt.getKey();
			String elementId = qual.getElementId();
			if (elementId.equals(oldElementId)) {
				continue;
			}
			// If duration item, select from 1/1 only.
			if (PeriodType.DURATION.equals(qual.getPeriodType())) {
				if (DateUtility.getMonth(qual.getStartDate()) != 1) {
					continue;
				}
			}
			PeriodType periodType = qual.getPeriodType();
			Date instant = qual.getInstant();
			Date startDate = qual.getStartDate();
			Date endDate = qual.getEndDate();
			ItemValue val = (ItemValue) qualValEnt.getValue();
			BigDecimal value = val.getValue();
			mainItemFam.set(elementId, periodType, instant, startDate, endDate,
					ver, value);
			oldElementId = elementId;
		}
	}

	private void generateMainRatioFamily(Xbrl entity, Date ver) {
		RatioFamily ratioFam = entity.getRatioFamily();
		MainRatioFamily mainRatioFam = entity.getMainRatioFamily();
		String oldElementId = null;
		for (Entry<RatioQualifier, RatioValue> qualValEnt : getLatestElementIdRecord(ratioFam)) {
			RatioQualifier qual = (RatioQualifier) qualValEnt.getKey();
			String elementId = qual.getElementId();
			if (elementId.equals(oldElementId)) {
				continue;
			}
			PeriodType periodType = qual.getPeriodType();
			Date instant = qual.getInstant();
			Date startDate = qual.getStartDate();
			Date endDate = qual.getEndDate();
			BigDecimal percent = ((RatioValue) qualValEnt.getValue())
					.getAsBigDecimal();
			mainRatioFam.setRatio(elementId, periodType, instant, startDate,
					endDate, ver, percent);
			oldElementId = elementId;
		}
	}

	private Set<Entry<RatioQualifier, RatioValue>> getLatestElementIdRecord(
			RatioFamily ratioFamily) {
		String OldElementId = null;
		Set<Entry<HBaseColumnQualifier, HBaseValue>> qualValSet = ratioFamily
				.getLatestQualifierAndValueAsDescendingSet();
		Map<RatioQualifier, RatioValue> map = new HashMap<RatioQualifier, RatioValue>(
				qualValSet.size());
		for (Entry<HBaseColumnQualifier, HBaseValue> qualValEnt : qualValSet) {
			RatioQualifier ratioQual = (RatioQualifier) qualValEnt.getKey();
			String elementId = ratioQual.getElementId();
			if (elementId.equals(OldElementId)) {
				continue;
			}
			if (PeriodType.DURATION.equals(ratioQual.getPeriodType())) {
				if (DateUtility.getMonth(ratioQual.getStartDate()) != 1) {
					continue;
				}
			}
			RatioValue ratioVal = (RatioValue) qualValEnt.getValue();
			map.put(ratioQual, ratioVal);
			OldElementId = elementId;
		}
		return map.entrySet();
	}

	private void generateItemFamily(Xbrl entity, Date ver) {
		ItemFamily itemFamily = entity.getItemFamily();
		for (Entry<HBaseColumnQualifier, NavigableMap<Date, HBaseValue>> qualEnt : entity
				.getInstanceFamily().getQualifierVersionValueSet()) {
			InstanceQualifier instQual = (InstanceQualifier) qualEnt.getKey();
			String elementId = instQual.getElementId();
			PeriodType periodType = instQual.getPeriodType();
			Date instant = instQual.getInstant();
			Date startDate = instQual.getStartDate();
			Date endDate = instQual.getEndDate();
			for (Entry<Date, HBaseValue> verEnt : qualEnt.getValue().entrySet()) {
				InstanceValue val = (InstanceValue) verEnt.getValue();
				BigDecimal value = getTwdValue(val.getUnitType(),
						val.getValue());
				itemFamily.set(elementId, periodType, instant, startDate,
						endDate, ver, value);
			}
		}
	}

	private void generateInstanceFamily(Xbrl entity, ObjectNode objNode,
			Date ver) throws ParseException {
		InstanceFamily instanceFamily = entity.getInstanceFamily();
		JsonNode instanceNode = objNode.get(InstanceAssistant.INSTANCE);
		Iterator<Entry<String, JsonNode>> fields = instanceNode.fields();
		while (fields.hasNext()) {
			Entry<String, JsonNode> eleIdEnt = fields.next();
			String eleId = eleIdEnt.getKey();
			ArrayNode arrNode = (ArrayNode) eleIdEnt.getValue();
			for (JsonNode dataNode : arrNode) {
				PeriodType periodType = PeriodType.getPeriodType(dataNode.get(
						"periodType").textValue());
				Date instant = getDate(dataNode.get("instant"));
				Date startDate = getDate(dataNode.get("startDate"));
				Date endDate = getDate(dataNode.get("endDate"));
				UnitType unitType = UnitType.getUnitType(dataNode.get("unit")
						.textValue());
				BigDecimal value = new BigDecimal(dataNode.get("value")
						.textValue());
				instanceFamily.setInstanceValue(eleId, periodType, instant,
						startDate, endDate, ver, unitType, value);
			}
		}
	}

	private void generateInfoFamilyVersion(Date ver, JsonNode infoNode,
			InfoFamily infoFamily) {
		String version = infoNode.get(InstanceAssistant.VERSION).textValue();
		infoFamily.setVersion(ver, XbrlTaxonomyVersion.valueOf(version));
	}

	private void generateInfoFamilyBalanceSheet(Date ver, JsonNode contextNode,
			InfoFamily infoFamily) {
		StringBuilder sb = new StringBuilder();
		JsonNode presentIdNode = contextNode.get(Presentation.Id.BalanceSheet);
		ArrayNode instantArrNode = (ArrayNode) presentIdNode
				.get(Instance.Attribute.INSTANT);
		sb.setLength(0);
		for (JsonNode context : instantArrNode) {
			sb.append(context.textValue() + COMMA_STRING);
		}
		infoFamily.setBalanceSheetContext(ver, sb.toString());
	}

	private void generateInfoFamilyStatementOfComprehensiveIncome(Date ver,
			JsonNode contextNode, InfoFamily infoFamily) {
		StringBuilder sb = new StringBuilder();
		JsonNode presentIdNode = contextNode
				.get(Presentation.Id.StatementOfComprehensiveIncome);
		ArrayNode instantArrNode = (ArrayNode) presentIdNode
				.get(Instance.Attribute.DURATION);
		sb.setLength(0);
		for (JsonNode context : instantArrNode) {
			sb.append(context.textValue() + COMMA_STRING);
		}
		infoFamily.setStatementOfComprehensiveIncomeContext(ver, sb.toString());
	}

	private void generateInfoFamilyStatementOfCashFlows(Date ver,
			JsonNode contextNode, InfoFamily infoFamily) {
		StringBuilder sb = new StringBuilder();
		JsonNode presentIdNode = contextNode
				.get(Presentation.Id.StatementOfCashFlows);
		ArrayNode instantArrNode = (ArrayNode) presentIdNode
				.get(Instance.Attribute.DURATION);
		sb.setLength(0);
		for (JsonNode context : instantArrNode) {
			sb.append(context.textValue() + COMMA_STRING);
		}
		infoFamily.setStatementOfCashFlowsContext(ver, sb.toString());
	}

	private void generateInfoFamilyStatementOfChangesInEquity(Date ver,
			JsonNode contextNode, InfoFamily infoFamily) {
		StringBuilder sb = new StringBuilder();
		JsonNode presentIdNode = contextNode
				.get(Presentation.Id.StatementOfChangesInEquity);
		ArrayNode instantArrNode = (ArrayNode) presentIdNode
				.get(Instance.Attribute.DURATION);
		sb.setLength(0);
		for (JsonNode context : instantArrNode) {
			sb.append(context.textValue() + COMMA_STRING);
		}
		infoFamily.setStatementOfChangesInEquityContext(ver, sb.toString());
	}

	private void generateInfoFamily(Xbrl entity, ObjectNode objNode, Date ver) {
		InfoFamily infoFamily = entity.getInfoFamily();
		JsonNode infoNode = objNode.get(InstanceAssistant.INFO);
		generateInfoFamilyVersion(ver, infoNode, infoFamily);
		JsonNode contextNode = infoNode.get(InstanceAssistant.CONTEXT);
		generateInfoFamilyBalanceSheet(ver, contextNode, infoFamily);
		generateInfoFamilyStatementOfComprehensiveIncome(ver, contextNode,
				infoFamily);
		generateInfoFamilyStatementOfCashFlows(ver, contextNode, infoFamily);
		generateInfoFamilyStatementOfChangesInEquity(ver, contextNode,
				infoFamily);
	}

	private void generateRowKey(Xbrl entity, String stockCode,
			ReportType reportType, int year, int season) {
		entity.new RowKey(stockCode, reportType, year, season, entity);
	}
}

package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.hbaseassistant.abstractclass.HBaseColumnQualifier;
import idv.hsiehpinghan.hbaseassistant.abstractclass.HBaseValue;
import idv.hsiehpinghan.mailassistant.assistant.MailAssistant;
import idv.hsiehpinghan.stockdao.entity.MainRatioAnalysis;
import idv.hsiehpinghan.stockdao.entity.MainRatioAnalysis.TTestFamily;
import idv.hsiehpinghan.stockdao.entity.MainRatioAnalysis.TTestFamily.TTestQualifier;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockdao.repository.MainRatioAnalysisRepository;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.TreeSet;

import javax.mail.MessagingException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StatisticAnalysisMailSender {
	// private Logger logger = Logger.getLogger(this.getClass().getName());
	private final String from = "daniel.hsiehpinghan@gmail.com";
	private final String to = "thank.hsiehpinghan@gmail.com";
	private final String subject = "會計科目比率變動";
	private final BigDecimal pValue = new BigDecimal("0.05");

	@Autowired
	private MainRatioAnalysisRepository analysisRepo;
	@Autowired
	private MailAssistant mailAssist;

	public boolean sendMainRatioAnalysis(String stockCode, ReportType reportType)
			throws MessagingException {
		TreeSet<MainRatioAnalysis> MainRatioAnalyses = analysisRepo.fuzzyScan(
				stockCode, reportType, null, null);
		if (MainRatioAnalyses.size() <= 0) {
			return false;
		}
		MainRatioAnalysis entity = MainRatioAnalyses.last();
		String content = generateMailContent(entity);
		if (content == null) {
			return false;
		}
		mailAssist.sendMail(from, to, generateSubject(entity), content, true);
		return true;
	}

	private String generateSubject(MainRatioAnalysis entity) {
		MainRatioAnalysis.RowKey rowKey = (MainRatioAnalysis.RowKey) entity
				.getRowKey();
		String stockCode = rowKey.getStockCode();
		ReportType reportType = rowKey.getReportType();
		int year = rowKey.getYear();
		int season = rowKey.getSeason();
		return String.format("%s年_第%s季_%s_%s報表_%s", year, season, stockCode,
				reportType.getChineseName(), subject);
	}

	private String generateMailContent(MainRatioAnalysis entity) {
		StringBuilder sb = new StringBuilder();
		sb.append("<html> ");
		sb.append("<body> ");
		MainRatioAnalysis.RowKey rowKey = (MainRatioAnalysis.RowKey) entity
				.getRowKey();
		sb.append(getUrl(rowKey));
		sb.append("<table style='border:3px #FFAC55 solid;'> ");
		sb.append("<thead> ");
		sb.append("<tr> ");
		sb.append("<th>項目</th> ");
		sb.append("<th width='70px'>P值</th> ");
		sb.append("<th width='70px'>自由度</th> ");
		sb.append("<th width='120px'>歷史平均數</th> ");
		sb.append("<th width='70px'>現值</th> ");
		sb.append("</tr> ");
		sb.append("</thead> ");
		sb.append("<tbody> ");
		TTestFamily tTestFam = entity.getTTestFamily();
		for (Entry<HBaseColumnQualifier, HBaseValue> ent : tTestFam
				.getLatestQualifierAndValueAsSet()) {
			TTestQualifier qual = (TTestQualifier) ent.getKey();
			String elementId = qual.getElementId();
			BigDecimal pValue = tTestFam.getPValue(elementId);
			if (this.pValue.compareTo(pValue) < 0) {
				continue;
			}
			String chineseName = tTestFam.getChineseName(elementId);
			BigDecimal degreeOfFreedom = tTestFam.getDegreeOfFreedom(elementId);
			BigDecimal sampleMean = tTestFam.getSampleMean(elementId);
			BigDecimal hypothesizedMean = tTestFam
					.getHypothesizedMean(elementId);
			DecimalFormat decimalFormat = new DecimalFormat("#0.00");
			sb.append("<tr> ");
			sb.append("<td align='left'> ");
			sb.append(chineseName);
			sb.append("</td> ");
			sb.append("<td align='right'> ");
			sb.append(decimalFormat.format(pValue));
			sb.append("</td> ");
			sb.append("<td align='right'> ");
			sb.append(degreeOfFreedom);
			sb.append("</td> ");
			sb.append("<td align='right'> ");
			sb.append(decimalFormat.format(sampleMean));
			sb.append("</td> ");
			sb.append("<td align='right'> ");
			sb.append(decimalFormat.format(hypothesizedMean));
			sb.append("</td> ");
			sb.append("</tr> ");
		}
		sb.append("</tbody> ");
		sb.append("</table> ");

		sb.append("</body> ");
		sb.append("</html> ");
		return sb.toString();
	}

	private String getUrl(MainRatioAnalysis.RowKey rowKey) {
		String stockCode = rowKey.getStockCode();
		ReportType reportType = rowKey.getReportType();
		int year = rowKey.getYear();
		int season = rowKey.getSeason();
		return String
				.format("<a href='http://localhost:8080/stare-project/stock/statisticAnalysis!query.action?request_locale=zh_TW&criteria.stockCode=%s&criteria.year=%s&criteria.season=%s&criteria.reportType=%s&criteria.pValue=%s'>前往網頁</a><br />",
						stockCode, year, season, reportType, pValue);
	}
}

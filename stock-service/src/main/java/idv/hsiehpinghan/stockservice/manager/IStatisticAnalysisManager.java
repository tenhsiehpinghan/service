package idv.hsiehpinghan.stockservice.manager;

import idv.hsiehpinghan.stockdao.entity.MainRatioAnalysis;
import idv.hsiehpinghan.stockdao.enumeration.ReportType;

import java.io.IOException;

public interface IStatisticAnalysisManager {
	boolean updateAnalyzedData() throws IOException;

	MainRatioAnalysis getMainRatioAnalysis(String stockCode,
			ReportType reportType, int year, int season);

	void sendMainRatioAnalysisMail();
}

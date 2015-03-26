package idv.hsiehpinghan.stockservice.property;

import java.io.File;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class StockServiceProperty implements InitializingBean {
	private final String EXCHANGE_RATE = "exchange_rate";
	private final String FINANCIAL_REPORT = "financial_report";
	private final String STOCK_CLOSING_CONDITION = "stock_closing_condition";
	private final String MONTHLY_OPERATING_INCOME = "monthly_operating_income";
	private final String COMPANY_BASIC_INFO = "company_basic_info";
	private final String BOT = "bot";
	private final String MOPS = "mops";
	private final String TWSE = "twse";
	private final String GRETAI = "gretai";
	private String downloadDir;
	private String extractDir;
	private String transportDir;
	private String rScriptDir;

	@Autowired
	private Environment environment;

	@Override
	public void afterPropertiesSet() throws Exception {
		processDownloadDir();
		processExtractDir();
		processTransportDir();
		processRScriptDir();
	}

	public File getStockClosingConditionDownloadDirOfTwse() {
		File dir = new File(downloadDir, TWSE);
		return new File(dir, STOCK_CLOSING_CONDITION);
	}

	public File getStockClosingConditionDownloadDirOfGretai() {
		File dir = new File(downloadDir, GRETAI);
		return new File(dir, STOCK_CLOSING_CONDITION);
	}

	public File getMonthlyOperatingIncomeDownloadDir() {
		File dir = new File(downloadDir, MOPS);
		return new File(dir, MONTHLY_OPERATING_INCOME);
	}

	public File getExchangeRateDownloadDir() {
		File dir = new File(downloadDir, BOT);
		return new File(dir, EXCHANGE_RATE);
	}

	public File getFinancialReportDownloadDir() {
		File dir = new File(downloadDir, MOPS);
		return new File(dir, FINANCIAL_REPORT);
	}

	public File getCompanyBasicInfoDownloadDir() {
		File dir = new File(downloadDir, MOPS);
		return new File(dir, COMPANY_BASIC_INFO);
	}

	public File getFinancialReportExtractDir() {
		File dir = new File(extractDir, MOPS);
		return new File(dir, FINANCIAL_REPORT);
	}

	public File getTransportDir() {
		return new File(transportDir);
	}

	public File getRScriptDir() {
		return new File(rScriptDir);
	}

	private void processDownloadDir() {
		String pDownloadDir = "stock_service_download_dir";
		downloadDir = environment.getProperty(pDownloadDir);
		if (downloadDir == null) {
			throw new RuntimeException(pDownloadDir + " not set !!!");
		}
	}

	private void processExtractDir() {
		String pExtractDir = "stock_service_extract_dir";
		extractDir = environment.getProperty(pExtractDir);
		if (extractDir == null) {
			throw new RuntimeException(pExtractDir + " not set !!!");
		}
	}

	private void processTransportDir() {
		String pTransportDir = "stock_service_transport_dir";
		transportDir = environment.getProperty(pTransportDir);
		if (transportDir == null) {
			throw new RuntimeException(pTransportDir + " not set !!!");
		}
	}

	private void processRScriptDir() {
		String pRScriptDir = "stock_service_r_script_dir";
		rScriptDir = environment.getProperty(pRScriptDir);
		if (rScriptDir == null) {
			throw new RuntimeException(pRScriptDir + " not set !!!");
		}
	}
}

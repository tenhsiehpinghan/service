package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.stockdao.enumeration.ReportType;
import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class XbrlTransporterTest {
	private String stockCode = "1256";
	private ReportType reportType = ReportType.CONSOLIDATED_STATEMENT;
	private XbrlTransporter transporter;

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		transporter = applicationContext.getBean(XbrlTransporter.class);
	}

	@Test
	public void saveHbaseDataToFile() throws Exception {
		File targetDirectory = new File(FileUtils.getTempDirectory(),
				"getXbrlFromHbase");
		transporter.saveHbaseDataToFile(stockCode, reportType, targetDirectory);
		File file = new File(targetDirectory, "xbrl");
		List<String> lines = FileUtils.readLines(file);
		Assert.assertTrue(lines.size() > 1);
	}
}

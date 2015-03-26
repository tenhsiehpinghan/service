package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.stockservice.suit.TestngSuitSetting;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.springframework.context.ApplicationContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MainRatioComputerTest {
	private MainRatioComputer computer;

	@BeforeClass
	public void beforeClass() throws IOException {
		ApplicationContext applicationContext = TestngSuitSetting
				.getApplicationContext();
		computer = applicationContext.getBean(MainRatioComputer.class);
	}

	@Test
	public void tTestMainRatio() throws Exception {
		File targetDirectory = new File("/tmp/getXbrlFromHbase");
		deleteResultFile(targetDirectory);
		File resultFile = computer.tTestMainRatio(targetDirectory);
		Assert.assertTrue(resultFile.exists());
	}

	private void deleteResultFile(File targetDirectory) {
		File resultFile = new File(targetDirectory, "result.csv");
		resultFile.delete();
	}
}

package idv.hsiehpinghan.stockservice.operator;

import idv.hsiehpinghan.compressutility.utility.CompressUtility;
import idv.hsiehpinghan.stockservice.property.StockServiceProperty;
import idv.hsiehpinghan.threadutility.utility.ThreadUtility;

import java.io.File;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FinancialReportUnzipper implements InitializingBean {
	private final int MAX_TRY_AMOUNT = 3;
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private File extractDir;

	@Autowired
	private StockServiceProperty stockServiceProperty;

	@Override
	public void afterPropertiesSet() throws Exception {
		extractDir = stockServiceProperty.getFinancialReportExtractDir();
	}

	/**
	 * Repeat try unzip.
	 * 
	 * @param file
	 * @return
	 */
	public File repeatTryUnzip(File file) {
		int tryAmount = 0;
		while (true) {
			File dir = null;
			try {
				dir = CompressUtility.unzip(file, extractDir, true);
				logger.info("Unzipp to " + dir + " success.");
				return dir;
			} catch (Exception e) {
				++tryAmount;
				logger.warn("Unzip fail " + tryAmount + " times !!!");
				if (tryAmount >= MAX_TRY_AMOUNT) {
					logger.warn("File("
							+ file.getAbsolutePath()
							+ ") delete "
							+ (file.delete() == true ? " success !!!"
									: " failed !!!"));
					logger.warn("Directory("
							+ dir.getAbsolutePath()
							+ ") delete "
							+ (dir.delete() == true ? " success !!!"
									: " failed !!!"));
					throw new RuntimeException(e);
				}
				ThreadUtility.sleep(tryAmount * 60);
			}
		}
	}

	/**
	 * Get extract directory.
	 * 
	 * @return
	 */
	public File getExtractDir() {
		return extractDir;
	}
}

package idv.hsiehpinghan.stockservice.utility;

import idv.hsiehpinghan.datatypeutility.utility.VoidUtility;
import idv.hsiehpinghan.stockservice.webelement.GretaiDatePickerTable;
import idv.hsiehpinghan.stockservice.webelement.XbrlDownloadTable;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openqa.selenium.support.ui.FluentWait;

import com.google.common.base.Function;

public class StockAjaxWaitUtility {
	private static final int POLLING_MILLISECONDS = 1000;
	private static final int TIMEOUT_MILLISECONDS = 10000;
	private static Logger logger = Logger.getLogger(StockAjaxWaitUtility.class
			.getName());

	/**
	 * Wait any button's onclick attribute like text.
	 * 
	 * @param table
	 * @param text
	 * @return
	 */
	public static boolean waitUntilAnyButtonOnclickAttributeLike(
			final XbrlDownloadTable table, final String regex) {
		return wait(new Function<Void, Boolean>() {
			@Override
			public Boolean apply(Void v) {
				try {
					return table.isAnyButtonOnclickAttributeLike(regex);
				} catch (Exception e) {
					logger.trace("Exception : ", e);
					return false;
				}
			}
		});
	}

	/**
	 * Wait untial all data year equal year.
	 * 
	 * @param table
	 * @param year
	 * @return
	 */
	public static boolean waitUntilAllDataYearEqual(
			final GretaiDatePickerTable table, final int year) {
		return wait(new Function<Void, Boolean>() {
			@Override
			public Boolean apply(Void v) {
				try {
					return table.isAllDataYearEquals(year);
				} catch (Exception e) {
					logger.trace("Exception : ", e);
					return false;
				}
			}
		});
	}

	/**
	 * Wait untial all data year equal year and all data month equal month.
	 * 
	 * @param table
	 * @param year
	 * @param month
	 * @return
	 */
	public static boolean waitUntilAllDataYearAndDataMonthEqual(
			final GretaiDatePickerTable table, final int year, final int month) {
		return wait(new Function<Void, Boolean>() {
			@Override
			public Boolean apply(Void v) {
				try {
					return table.isAllDataYearAndDataMonthEquals(year, month);
				} catch (Exception e) {
					logger.trace("Exception : ", e);
					return false;
				}
			}
		});
	}

	private static boolean wait(final Function<Void, Boolean> function) {
		FluentWait<Void> fluentWait = new FluentWait<Void>(VoidUtility.VOID);
		fluentWait.pollingEvery(POLLING_MILLISECONDS, TimeUnit.MILLISECONDS);
		fluentWait.withTimeout(TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
		return fluentWait.until(function);
	}
}

package idv.hsiehpinghan.stockservice.webelement;

import idv.hsiehpinghan.datatypeutility.utility.StringUtility;
import idv.hsiehpinghan.seleniumassistant.webelement.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ListUtils;
import org.openqa.selenium.WebElement;

public class MonthlyOperatingIncomeDownloadTable extends Table {
	private static final String EMPTY_STRING = StringUtility.EMPTY_STRING;
	private static final String COMMA_STRING = StringUtility.COMMA_STRING;
	private static final String FULL_WIDTH_COMMA_STRING = StringUtility.FULL_WIDTH_COMMA_STRING;
	private static final int DATA_ROW_BEGIN_INDEX_1 = 1;
	private static final int DATA_ROW_BEGIN_INDEX_2 = 2;
	private static final String[] itemNames1 = { "本月", "去年同期", "增減金額", "增減百分比",
			"本年累計", "去年累計", "增減金額", "增減百分比", "備註" };
	private static final String[] itemNames2 = { "本月", "去年同期", "增減金額", "增減百分比",
			"本年累計", "去年累計", "增減金額", "增減百分比", "本月換算匯率：", "本年累計換算匯率：", "備註" };
	private List<String> targetRowTexts1;
	private List<String> targetRowTexts2;
	private Map<String, MonthlyOperatingIncome> monthlyOperatingIncomes;

	public MonthlyOperatingIncomeDownloadTable(Table table) {
		super(table.getWebDriver(), table.getBy());
		int type = checkTitle();
		if (type == 1) {
			checkItemName1();
			generateMonthlyOperatingIncomes1();
		} else if (type == 2) {
			checkItemName2();
			generateMonthlyOperatingIncomes2();
		} else {
			throw new RuntimeException("Type(" + type + ") not implements !!!");
		}

	}

	/**
	 * Get expected type 1 row texts.
	 * 
	 * @return
	 */
	public List<String> getTargetRowTexts1() {
		if (targetRowTexts1 == null) {
			targetRowTexts1 = new ArrayList<String>(2);
			targetRowTexts1.add("項目");
			targetRowTexts1.add("營業收入淨額");
		}
		return targetRowTexts1;
	}

	/**
	 * Get expected type 2 row texts.
	 * 
	 * @return
	 */
	public List<String> getTargetRowTexts2() {
		if (targetRowTexts2 == null) {
			targetRowTexts2 = new ArrayList<String>(2);
			targetRowTexts2.add("項目");
			targetRowTexts2.add("合併營業收入淨額");
		}
		return targetRowTexts2;
	}

	public static String[] getItemNames2() {
		return itemNames2;
	}

	public Map<String, MonthlyOperatingIncome> getMonthlyOperatingIncomes() {
		return monthlyOperatingIncomes;
	}

	private int checkTitle() {
		List<String> titles = getRowAsStringList(0);
		List<String> targetTitles1 = getTargetRowTexts1();
		if (ListUtils.isEqualList(titles, targetTitles1)) {
			return 1;
		}
		List<String> targetTitles2 = getTargetRowTexts2();
		if (ListUtils.isEqualList(titles, targetTitles2)) {
			return 2;
		}
		throw new RuntimeException("Titles(" + titles
				+ ") different from targetTitles1(" + targetTitles1
				+ ") and targetTitles2(" + targetTitles2 + ") !!!");
	}

	private void checkItemName1() {
		// i = 0 is title.
		for (int i = 1, size = 9; i < size; ++i) {
			String itemName = getTextOfCell(i, 0);
			String targetItemName = itemNames1[i - DATA_ROW_BEGIN_INDEX_1];
			if (targetItemName.equals(itemName) == false) {
				throw new RuntimeException("TargetItemName(" + targetItemName
						+ ") different from itemName(" + itemName + ") !!!");
			}
		}
	}

	private void checkItemName2() {
		// i = 0 is title, i = 1 is currency type.
		for (int i = 2, size = 12; i < size; ++i) {
			String itemName = getTextOfCell(i, 0);
			String targetItemName = itemNames2[i - DATA_ROW_BEGIN_INDEX_2];
			if (targetItemName.equals(itemName) == false) {
				throw new RuntimeException("TargetItemName(" + targetItemName
						+ ") different from itemName(" + itemName + ") !!!");
			}
		}
	}

	private void generateMonthlyOperatingIncomes1() {
		// Column 0 is item name.
		String currentMonth = getTextOfCell(1, 1).trim();
		String currentMonthOfLastYear = getTextOfCell(2, 1).trim();
		String differentAmount = getTextOfCell(3, 1).trim();
		String differentPercent = getTextOfCell(4, 1).trim();
		String cumulativeAmountOfThisYear = getTextOfCell(5, 1).trim();
		String cumulativeAmountOfLastYear = getTextOfCell(6, 1).trim();
		String cumulativeDifferentAmount = getTextOfCell(7, 1).trim();
		String cumulativeDifferentPercent = getTextOfCell(8, 1).trim();
		String comment = getTextOfCell(9, 1).trim();
		monthlyOperatingIncomes = new HashMap<String, MonthlyOperatingIncome>(1);
		MonthlyOperatingIncome monthlyOperatingIncome = new MonthlyOperatingIncome(
				currentMonth, currentMonthOfLastYear, differentAmount,
				differentPercent, cumulativeAmountOfThisYear,
				cumulativeAmountOfLastYear, cumulativeDifferentAmount,
				cumulativeDifferentPercent, comment);
		monthlyOperatingIncomes.put(EMPTY_STRING, monthlyOperatingIncome);
	}

	private void generateMonthlyOperatingIncomes2() {
		int currencyRowindex = 1;
		List<WebElement> currencyTitles = getColumns(currencyRowindex);
		int titleSize = currencyTitles.size();
		monthlyOperatingIncomes = new HashMap<String, MonthlyOperatingIncome>(
				titleSize);
		// Column 0 is item name.
		for (int i = 1, size = titleSize + 1; i < size; ++i) {
			// Because title row only tow columns.
			String currencyTitle = currencyTitles.get(i - 1).getText();
			String currentMonth = getTextOfCell(2, i).trim();
			String currentMonthOfLastYear = getTextOfCell(3, i).trim();
			String differentAmount = getTextOfCell(4, i).trim();
			String differentPercent = getTextOfCell(5, i).trim();
			String cumulativeAmountOfThisYear = getTextOfCell(6, i).trim();
			String cumulativeAmountOfLastYear = getTextOfCell(7, i).trim();
			String cumulativeDifferentAmount = getTextOfCell(8, i).trim();
			String cumulativeDifferentPercent = getTextOfCell(9, i).trim();
			String exchangeRateOfCurrentMonth = getTextOfCell(10, i).trim();
			String cumulativeExchangeRateOfCurrentYear = getTextOfCell(11, i)
					.trim();
			String comment = getTextOfCell(12, i).trim();
			MonthlyOperatingIncome monthlyOperatingIncome = new MonthlyOperatingIncome(
					currentMonth, currentMonthOfLastYear, differentAmount,
					differentPercent, cumulativeAmountOfThisYear,
					cumulativeAmountOfLastYear, cumulativeDifferentAmount,
					cumulativeDifferentPercent, exchangeRateOfCurrentMonth,
					cumulativeExchangeRateOfCurrentYear, comment);
			monthlyOperatingIncomes.put(currencyTitle, monthlyOperatingIncome);
		}
	}

	public class MonthlyOperatingIncome {
		private String currentMonth = EMPTY_STRING;
		private String currentMonthOfLastYear = EMPTY_STRING;
		private String differentAmount = EMPTY_STRING;
		private String differentPercent = EMPTY_STRING;
		private String cumulativeAmountOfThisYear = EMPTY_STRING;
		private String cumulativeAmountOfLastYear = EMPTY_STRING;
		private String cumulativeDifferentAmount = EMPTY_STRING;
		private String cumulativeDifferentPercent = EMPTY_STRING;
		private String exchangeRateOfCurrentMonth = EMPTY_STRING;
		private String cumulativeExchangeRateOfCurrentYear = EMPTY_STRING;
		private String comment = EMPTY_STRING;

		public MonthlyOperatingIncome(String currentMonth,
				String currentMonthOfLastYear, String differentAmount,
				String differentPercent, String cumulativeAmountOfThisYear,
				String cumulativeAmountOfLastYear,
				String cumulativeDifferentAmount,
				String cumulativeDifferentPercent, String comment) {
			super();
			this.currentMonth = currentMonth;
			this.currentMonthOfLastYear = currentMonthOfLastYear;
			this.differentAmount = differentAmount;
			this.differentPercent = differentPercent;
			this.cumulativeAmountOfThisYear = cumulativeAmountOfThisYear;
			this.cumulativeAmountOfLastYear = cumulativeAmountOfLastYear;
			this.cumulativeDifferentAmount = cumulativeDifferentAmount;
			this.cumulativeDifferentPercent = cumulativeDifferentPercent;
			this.comment = comment;
		}

		public MonthlyOperatingIncome(String currentMonth,
				String currentMonthOfLastYear, String differentAmount,
				String differentPercent, String cumulativeAmountOfThisYear,
				String cumulativeAmountOfLastYear,
				String cumulativeDifferentAmount,
				String cumulativeDifferentPercent,
				String exchangeRateOfCurrentMonth,
				String cumulativeExchangeRateOfCurrentYear, String comment) {
			super();
			this.currentMonth = currentMonth;
			this.currentMonthOfLastYear = currentMonthOfLastYear;
			this.differentAmount = differentAmount;
			this.differentPercent = differentPercent;
			this.cumulativeAmountOfThisYear = cumulativeAmountOfThisYear;
			this.cumulativeAmountOfLastYear = cumulativeAmountOfLastYear;
			this.cumulativeDifferentAmount = cumulativeDifferentAmount;
			this.cumulativeDifferentPercent = cumulativeDifferentPercent;
			this.exchangeRateOfCurrentMonth = exchangeRateOfCurrentMonth;
			this.cumulativeExchangeRateOfCurrentYear = cumulativeExchangeRateOfCurrentYear;
			this.comment = comment;
		}

		public String getCurrentMonth() {
			if (currentMonth == null) {
				throw new RuntimeException("CurrentMonth is null !!!");
			}
			return currentMonth;
		}

		public String getCurrentMonthOfLastYear() {
			if (currentMonthOfLastYear == null) {
				throw new RuntimeException("CurrentMonthOfLastYear is null !!!");
			}
			return currentMonthOfLastYear;
		}

		public String getDifferentAmount() {
			if (differentAmount == null) {
				throw new RuntimeException("DifferentAmount is null !!!");
			}
			return differentAmount;
		}

		public String getDifferentPercent() {
			if (differentPercent == null) {
				throw new RuntimeException("DifferentPercent is null !!!");
			}
			return differentPercent;
		}

		public String getCumulativeAmountOfThisYear() {
			if (cumulativeAmountOfThisYear == null) {
				throw new RuntimeException(
						"CumulativeAmountOfThisYear is null !!!");
			}
			return cumulativeAmountOfThisYear;
		}

		public String getCumulativeAmountOfLastYear() {
			if (cumulativeAmountOfLastYear == null) {
				throw new RuntimeException(
						"CumulativeAmountOfLastYear is null !!!");
			}
			return cumulativeAmountOfLastYear;
		}

		public String getCumulativeDifferentAmount() {
			if (cumulativeDifferentAmount == null) {
				throw new RuntimeException(
						"CumulativeDifferentAmount is null !!!");
			}
			return cumulativeDifferentAmount;
		}

		public String getCumulativeDifferentPercent() {
			if (cumulativeDifferentPercent == null) {
				throw new RuntimeException(
						"CumulativeDifferentPercent is null !!!");
			}
			return cumulativeDifferentPercent;
		}

		public String getExchangeRateOfCurrentMonth() {
			if (exchangeRateOfCurrentMonth == null) {
				throw new RuntimeException(
						"exchangeRateOfCurrentMonth is null !!!");
			}
			return exchangeRateOfCurrentMonth;
		}

		public String getCumulativeExchangeRateOfCurrentYear() {
			if (cumulativeExchangeRateOfCurrentYear == null) {
				throw new RuntimeException(
						"cumulativeExchangeRateOfCurrentYear is null !!!");
			}
			return cumulativeExchangeRateOfCurrentYear;
		}

		public String getComment() {
			if (comment == null) {
				throw new RuntimeException("Comment is null !!!");
			}
			return comment.replace(COMMA_STRING, FULL_WIDTH_COMMA_STRING);
		}

	}
}

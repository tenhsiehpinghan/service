package idv.hsiehpinghan.stockservice.webelement;

import idv.hsiehpinghan.seleniumassistant.webelement.Table;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class ExchangeRateDownloadTable extends Table {
	private final int DATE_COLUMN_INDEX = 0;
	private final int EXCHANGE_RATE_COLUMN_INDEX = 4;
	private List<String> targetRow0Texts;
	private List<String> targetRow1Texts;

	public ExchangeRateDownloadTable(Table table) {
		super(table.getWebDriver(), table.getBy());
	}

	/**
	 * Get exchange rate date.
	 * 
	 * @param rowIndex
	 * @return
	 * @throws ParseException
	 */
	public String getDate(int rowIndex) throws ParseException {
		return getTextOfCell(rowIndex, DATE_COLUMN_INDEX);
	}

	/**
	 * Get exchange rate.
	 * 
	 * @param rowIndex
	 * @return
	 * @throws ParseException
	 */
	public String getExchangeRate(int rowIndex) throws ParseException {
		return getTextOfCell(rowIndex, EXCHANGE_RATE_COLUMN_INDEX);
	}

	/*
	 * Get expected row 0 texts.
	 * 
	 * @return
	 */
	public List<String> getTargetRow0Texts() {
		if (targetRow0Texts == null) {
			targetRow0Texts = new ArrayList<String>(4);
			targetRow0Texts.add("掛牌日期");
			targetRow0Texts.add("幣別");
			targetRow0Texts.add("現金匯率");
			targetRow0Texts.add("即期匯率");
		}
		return targetRow0Texts;
	}

	/**
	 * Get expected row 1 texts.
	 * 
	 * @return
	 */
	public List<String> getTargetRow1Texts() {
		if (targetRow1Texts == null) {
			targetRow1Texts = new ArrayList<String>(4);
			targetRow1Texts.add("買入");
			targetRow1Texts.add("賣出");
			targetRow1Texts.add("買入");
			targetRow1Texts.add("賣出");
		}
		return targetRow1Texts;
	}

}

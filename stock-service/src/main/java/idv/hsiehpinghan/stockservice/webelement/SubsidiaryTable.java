package idv.hsiehpinghan.stockservice.webelement;

import idv.hsiehpinghan.seleniumassistant.webelement.Table;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.ListUtils;

public class SubsidiaryTable extends Table {
	private static final int STOCK_CODE_COLUMN_INDEX = 0;
	private static final int QUERY_BUTTON_COLUMN_INDEX = 2;
	private List<String> targetRowTexts;

	public SubsidiaryTable(Table table) {
		super(table.getWebDriver(), table.getBy());
		checkTitle();
	}

	public List<String> getTargetRowTexts() {
		if (targetRowTexts == null) {
			targetRowTexts = new ArrayList<String>(3);
			targetRowTexts.add("公司代號");
			targetRowTexts.add("公司名稱");
			targetRowTexts.add(" ");
		}
		return targetRowTexts;
	}

	private void checkTitle() {
		List<String> titles = getRowAsStringList(0);
		List<String> targetTitles = getTargetRowTexts();
		if (ListUtils.isEqualList(titles, targetTitles) == false) {
			throw new RuntimeException("TargetTitles(" + targetTitles
					+ ") different from titles(" + titles + ") !!!");
		}
	}

	public void clickQueryButton(int rowIndex) {
		this.clickButtonOfCell(rowIndex, QUERY_BUTTON_COLUMN_INDEX);
	}

	public String getStockCode(int rowIndex) {
		return getTextOfCell(rowIndex, STOCK_CODE_COLUMN_INDEX);
	}

}

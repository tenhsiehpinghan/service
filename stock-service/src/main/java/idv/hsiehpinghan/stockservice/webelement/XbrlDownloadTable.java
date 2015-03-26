package idv.hsiehpinghan.stockservice.webelement;

import idv.hsiehpinghan.seleniumassistant.webelement.Table;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.ListUtils;

public class XbrlDownloadTable extends Table {
	private final int DOWNLOAD_BUTTON_COLUMN_INDEX = 2;
	private final String ONCLICK = "onclick";
	private List<String> targetRowTexts;

	public XbrlDownloadTable(Table table) {
		super(table.getWebDriver(), table.getBy());
	}

	/**
	 * Get download file name. If return null means no file could be downloaded.
	 * 
	 * @param rowIndex
	 * @return
	 */
	public String getDownloadFileName(int rowIndex) {
		// ex :
		// onclick="document.t164form.fileName.value = '2013-01-otc-02-C.zip';document.t164form.action = '/server-java/FileDownLoad';document.t164form.submit();"
		String attrValue = this.getButtonAttributeOfCell(rowIndex,
				DOWNLOAD_BUTTON_COLUMN_INDEX, ONCLICK);
		String fileName = getFileName(attrValue);
		// Some industry type has no data.
		if ("null".equals(fileName)) {
			return null;
		}
		return fileName;
	}

	/**
	 * Click download button.
	 * 
	 * @param rowIndex
	 */
	public void clickDownloadButton(int rowIndex) {
		this.clickButtonOfCell(rowIndex, DOWNLOAD_BUTTON_COLUMN_INDEX);
	}

	/**
	 * Get expected row texts.
	 * 
	 * @return
	 */
	public List<String> getTargetRowTexts() {
		if (targetRowTexts == null) {
			targetRowTexts = new ArrayList<String>(3);
			targetRowTexts.add("代號");
			targetRowTexts.add("產業別");
			targetRowTexts.add("下載");
		}
		return targetRowTexts;
	}

	/**
	 * Check is any button on click attribute like regex.
	 * 
	 * @param regex
	 * @return
	 */
	public boolean isAnyButtonOnclickAttributeLike(String regex) {
		for (int i = 1, size = getRowSize(); i < size; ++i) {
			String fileName = getDownloadFileName(i);
			if (fileName != null && fileName.matches(regex)) {
				return true;
			}
		}
		return false;
	}

	public void checkTitles() {
		List<String> titles = getRowAsStringList(0);
		List<String> targetTitles = getTargetRowTexts();
		if (ListUtils.isEqualList(titles, targetTitles) == false) {
			throw new RuntimeException("TargetTitles(" + targetTitles
					+ ") different from titles(" + titles + ") !!!");
		}
	}

	private String getFileName(String attrValue) {
		int idxBegin = attrValue.indexOf("'") + 1;
		int idxEnd = attrValue.indexOf("'", idxBegin);
		return attrValue.substring(idxBegin, idxEnd);
	}

}

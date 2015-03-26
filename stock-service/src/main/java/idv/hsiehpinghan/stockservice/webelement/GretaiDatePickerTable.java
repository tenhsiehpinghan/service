package idv.hsiehpinghan.stockservice.webelement;

import idv.hsiehpinghan.seleniumassistant.webelement.Table;

import java.util.ArrayList;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

public class GretaiDatePickerTable extends Table {
	private static final String DATA_YEAR = "data-year";
	private static final String DATA_MONTH = "data-month";
	
	public GretaiDatePickerTable(Table table) {
		super(table.getWebDriver(), table.getBy());
	}

	public boolean isAllDataYearEquals(int year) {
		List<WebElement> tds = getTds();
		String targetYear = String.valueOf(year);
		for (WebElement td : tds) {
			String dataYear = td.getAttribute(DATA_YEAR);
			if (dataYear == null) {
				continue;
			}
			if (targetYear.equals(dataYear) == false) {
				return false;
			}
		}
		return true;
	}

	public boolean isAllDataYearAndDataMonthEquals(int year, int month) {
		List<WebElement> tds = getTds();
		String targetYear = String.valueOf(year);
		String targetMonth = String.valueOf(month);
		for (WebElement td : tds) {
			String dataYear = td.getAttribute(DATA_YEAR);
			String dataMonth = td.getAttribute(DATA_MONTH);
			if (dataYear == null && dataMonth == null) {
				continue;
			}
			if (targetYear.equals(dataYear) == false) {
				return false;
			}
			if(targetMonth.equals(dataMonth) == false) {
				return false;
			}
		}
		return true;
	}
	
	public void clickDayOfMonth(int dayOfMonth) {
		List<WebElement> tds = getTds();
		String targetDayOfMonth = String.valueOf(dayOfMonth);
		for (WebElement td : tds) {
			if(targetDayOfMonth.equals(td.getText())) {
				td.click();
				break;
			}
		}
	}
	
	private List<WebElement> getTds() {
		List<WebElement> trs = getTrs();
		List<WebElement> sumTds = new ArrayList<WebElement>();
		for (WebElement tr : trs) {
			List<WebElement> tds = tr.findElements(By.cssSelector("td"));
			sumTds.addAll(tds);
		}
		return sumTds;
	}

	private List<WebElement> getTrs() {
		WebElement tbody = getTbody();
		return tbody.findElements(By.cssSelector("tr"));
	}

	private WebElement getTbody() {
		return getSeleniumWebElement().findElements(By.cssSelector("tbody"))
				.get(0);
	}
}

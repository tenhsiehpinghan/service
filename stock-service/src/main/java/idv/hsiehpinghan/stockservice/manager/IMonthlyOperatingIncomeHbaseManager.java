package idv.hsiehpinghan.stockservice.manager;

import idv.hsiehpinghan.stockdao.entity.MonthlyOperatingIncome;

import java.util.TreeSet;

public interface IMonthlyOperatingIncomeHbaseManager {
	boolean updateMonthlyOperatingIncome();

	TreeSet<MonthlyOperatingIncome> getAll(String stockCode,
			boolean isFunctionalCurrency);
}

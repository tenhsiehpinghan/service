package idv.hsiehpinghan.stockservice.manager;

import idv.hsiehpinghan.stockdao.entity.StockClosingCondition;

import java.util.TreeSet;

public interface IStockClosingConditionManager {
	boolean updateStockClosingCondition();

	TreeSet<StockClosingCondition> getAll(String stockCode);
}

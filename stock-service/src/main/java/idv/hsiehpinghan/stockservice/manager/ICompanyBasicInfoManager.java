package idv.hsiehpinghan.stockservice.manager;

import idv.hsiehpinghan.stockdao.entity.StockInfo;

public interface ICompanyBasicInfoManager {
	boolean updateCompanyBasicInfo();

	StockInfo getStockInfo(String stockCode);
}

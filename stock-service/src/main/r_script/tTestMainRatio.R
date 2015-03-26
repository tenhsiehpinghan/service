if(exists('xbrlFile') == FALSE) {
	msg <- 'xbrlFile not exists !!!'
	print(msg)
	stop(msg)
}
if(exists('resultFile') == FALSE) {
	msg <- 'resultFile not exists !!!'
	print(msg)
	stop(msg)
}
(function(parameter) {
		df <- read.csv(xbrlFile, colClasses=c(
						'stockCode'='character', 
						'reportType'='character', 
						'year'='integer', 
						'season'='integer', 
						'elementId'='character', 
						'chineseName'='character', 
						'englishName'='character', 
						'ratio'='numeric'
				), sep='\t'
		)
		target_year_season <- max(df$year*100 + df$season)
		target_year <- target_year_season %/% 100
		target_season <- target_year_season %% 100
		target_elements <- subset(df, subset=(year==target_year & season==target_season))
		history_elements <- subset(df, subset=(!(year==target_year & season==target_season)), select=c(elementId, ratio))
		history_groups <- split(history_elements, history_elements$elementId)
		length = nrow(target_elements)
		tempDf <- data.frame(
				stockCode = character(length),
				reportType = character(length),
				year = integer(length),
				season = integer(length),
				elementId = character(length),	
				chineseName = character(length),
				englishName = character(length),
				statistic = numeric(length),
				degreeOfFreedom = numeric(length),
				confidenceInterval = numeric(length),
				sampleMean = numeric(length),
				hypothesizedMean = numeric(length),
				pValue = numeric(length),
				stringsAsFactors=FALSE
		)
		
		for(i in 1:nrow(target_elements)) {
			targetRow <- target_elements[i,]
			ratio = targetRow$ratio
			group <- history_groups[targetRow$elementId]
			diffs <- group[[1]]$ratio
			if(length(diffs) < 2) {
				next
			}
			try({
				htest <- t.test(diffs, mu=ratio)
				if(is.na(htest$p.value)) {
					next
				}
				if(is.infinite(htest$statistic)) {
					next
				}
				tempDf$stockCode[i] <- targetRow$stockCode
				tempDf$reportType[i] <- targetRow$reportType
				tempDf$year[i] <- targetRow$year
				tempDf$season[i] <- targetRow$season
				tempDf$elementId[i] <- targetRow$elementId
				tempDf$chineseName[i] <- targetRow$chineseName
				tempDf$englishName[i] <- targetRow$englishName
				tempDf$statistic[i] <- htest$statistic
				tempDf$degreeOfFreedom[i] <- htest$parameter
				tempDf$confidenceInterval[i] <- htest$conf.int
				tempDf$sampleMean[i] <- htest$estimate
				tempDf$hypothesizedMean[i] <- htest$null.value
				tempDf$pValue[i] <- htest$p.value
			})
		}
		resultDf <- subset(tempDf, subset=(stockCode != ''))
		write.csv(resultDf, file=resultFile, row.names = FALSE)
	})()
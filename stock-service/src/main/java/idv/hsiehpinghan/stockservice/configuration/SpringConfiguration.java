package idv.hsiehpinghan.stockservice.configuration;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@Configuration("stockServiceSpringConfiguration")
@PropertySource("classpath:/stock_service.property")
@ComponentScan(basePackages = { "idv.hsiehpinghan.stockservice" })
public class SpringConfiguration {
	// private Logger logger = Logger.getLogger(this.getClass().getName());

}

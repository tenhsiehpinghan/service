package idv.hsiehpinghan.stockservice.suit;

import idv.hsiehpinghan.objectutility.utility.ClassUtility;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.BeforeSuite;

public class TestngSuitSetting {
	private static AnnotationConfigApplicationContext applicationContext;

	@BeforeSuite()
	public void beforeSuite() throws Exception {
		Class<?>[] clsArr = ClassUtility.getAnnotatedClasses(
				"idv.hsiehpinghan", Configuration.class);
		applicationContext = new AnnotationConfigApplicationContext(clsArr);
	}

	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}

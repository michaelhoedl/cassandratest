package utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class DateHelper {

	private static final DateFormat dateFormat = new SimpleDateFormat(
			"dd.MM.yyyy HH:mm:ss");
	
	public static String getCurrentTimeStampAsString() {
		java.util.Date today = new java.util.Date();
		return dateFormat.format(today.getTime());
	}
	
	public static Date getCurrentTimeStampAsDate() {
		return new java.util.Date();
	}
}

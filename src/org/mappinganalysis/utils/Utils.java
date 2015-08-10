package org.mappinganalysis.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class Utils {

	public static HttpURLConnection openUrlConnection(URL url) {
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
		
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}

}

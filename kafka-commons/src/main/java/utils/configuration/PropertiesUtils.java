package utils.configuration;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public final class PropertiesUtils {

  private static final PropertiesConfiguration CONFIG = new PropertiesConfiguration();

  static {
    try {
      CONFIG.load("src/main/resources/application.properties");
    }
    catch (ConfigurationException e) {
      e.printStackTrace();
    }
  }

  public static String getProperty(String key) {
    return CONFIG.getString(key);
  }
}

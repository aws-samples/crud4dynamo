package com.amazon.crud4dynamo.testbase;

import com.google.common.io.Resources;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class Log4jEnabledTestBase {

  private static final String LOG4J_CONFIGURATION_FILE_SYSTEM_PROPERTY = "log4j.configurationFile";
  private static final String LOG4J_CONFIG_NAME = "log4j2-test.xml";

  @BeforeAll
  public static void beforeClass() {
    System.setProperty(
        LOG4J_CONFIGURATION_FILE_SYSTEM_PROPERTY,
        Resources.getResource(LOG4J_CONFIG_NAME).getFile());
  }

  @AfterAll
  public static void afterClass() {
    System.clearProperty(LOG4J_CONFIGURATION_FILE_SYSTEM_PROPERTY);
  }
}

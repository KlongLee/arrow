/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.driver.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Collection;
import java.util.Map;

import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link ArrowFlightJdbcDriver}.
 */
public class ArrowFlightJdbcDriverTest {

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE;
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user("user1", "pass1").user("user2", "pass2")
            .build();

    FLIGHT_SERVER_TEST_RULE = new FlightServerTestRule.Builder().host("localhost").randomPort()
        .authentication(authentication).producer(PRODUCER).build();
  }

  private BufferAllocator allocator;
  private ArrowFlightJdbcConnectionPoolDataSource dataSource;

  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    dataSource = FLIGHT_SERVER_TEST_RULE.createConnectionPoolDataSource();
  }

  @After
  public void tearDown() throws Exception {
    Collection<BufferAllocator> childAllocators = allocator.getChildAllocators();
    AutoCloseables.close(childAllocators.toArray(new AutoCloseable[0]));
    AutoCloseables.close(dataSource, allocator);
  }

  @Test
  public void testBallista() throws Exception {
    String url = "jdbc:arrow-flight://localhost:50050";
    java.util.Properties props = new java.util.Properties();
    props.setProperty("useEncryption", "false");
    props.setProperty("user", "admin");
    props.setProperty("password", "password");
    Connection con = DriverManager.getConnection(url, props);

    // Do exact same sequence as DataGrip does
    assertFalse(con.isClosed());
    DatabaseMetaData dbmd = con.getMetaData();
    assertEquals("Arrow Flight JDBC Driver", dbmd.getDriverName());
    assertEquals("9.0.0-SNAPSHOT", dbmd.getDriverVersion());
    //assertEquals("", md.getDatabaseProductName()); // UNIMPLEMENTED: Implement CommandGetSqlInfo
    assertEquals(9, dbmd.getDatabaseMajorVersion());
    assertEquals(0, dbmd.getDatabaseMinorVersion());
    assertNull(con.getClientInfo("ApplicationName"));
//    con.setClientInfo("ApplicationName", "DataGrip 2022.2.1");
    assertEquals(4, dbmd.getJDBCMajorVersion());
    assertNotNull(dbmd.getConnection());
//    assertEquals("", md.getIdentifierQuoteString()); // Implement CommandGetSqlInfo
//    assertEquals("", md.getExtraNameCharacters()); // Implement CommandGetSqlInfo
    assertFalse(dbmd.supportsMixedCaseIdentifiers());
    assertTrue(dbmd.storesUpperCaseIdentifiers());
    assertFalse(dbmd.storesLowerCaseIdentifiers());
    assertFalse(dbmd.storesMixedCaseIdentifiers());
    assertTrue(dbmd.supportsMixedCaseQuotedIdentifiers());
    assertFalse(dbmd.storesUpperCaseQuotedIdentifiers());
    assertFalse(dbmd.storesLowerCaseQuotedIdentifiers());
    assertFalse(dbmd.storesMixedCaseQuotedIdentifiers());
    assertTrue(con.isValid(20));
//    assertFalse(md.supportsSavepoints()); // Implement CommandGetSqlInfo
    assertTrue(con.getAutoCommit());
    con.setReadOnly(false);
    assertNull(con.getCatalog());
    assertNull(con.getSchema());

    // statement
    java.sql.Statement stmt = con.createStatement();
//    stmt.setEscapeProcessing(false); // Avatica not supported
    assertFalse(stmt.isClosed());
    stmt.setFetchSize(100);
    String sql = "create external table customer STORED AS CSV WITH HEADER ROW\n" +
            "    LOCATION '/home/bgardner/workspace/ballista/arrow-datafusion/datafusion/core/tests/tpch-csv/customer.csv';\n";
    assertFalse(stmt.execute(sql));

    // resultset
    ResultSet rs = stmt.getResultSet();
    assertFalse(rs.isClosed()); // NPE
    ResultSetMetaData md = rs.getMetaData();
    assertNotNull(md);
    assertEquals(1, md.getColumnCount());
    assertEquals("", md.getColumnTypeName(1));
    assertEquals("", md.getColumnType(1));
    assertEquals("", md.getColumnClassName(1));
    assertEquals(1, rs.getType());
    assertFalse(rs.next());
    assertFalse(rs.isClosed());
    assertFalse(stmt.getMoreResults());
    assertFalse(stmt.isClosed());
    assertNull(con.getWarnings());
    con.clearWarnings();;
    stmt.close();

    int updateCount = stmt.getUpdateCount();
    assertEquals(updateCount, 0);

    sql = "select c_name from customer order by c_name limit 1";
    assertTrue(stmt.execute(sql));
    rs = stmt.getResultSet();
    int count = 0;
    while(rs.next()) {
      String key = rs.getString(1);
      assertEquals(key, "Customer#000000002");
      count++;
    }
    assertEquals(count, 1);
  }

  @Test
  public void testDremio() throws Exception {
    String sql = "select 'Hello, Dremio';";
    String url = "jdbc:arrow-flight://localhost:32010";
    java.util.Properties props = new java.util.Properties();
    props.setProperty("useEncryption", "false");
    props.setProperty("user", "dremio");
    props.setProperty("password", "dremio123");
    Connection con = DriverManager.getConnection(url, props);
    java.sql.Statement stmt = con.createStatement();
    boolean result = stmt.execute(sql);
    assertEquals(result, true);
    java.sql.ResultSet rs = stmt.getResultSet();
    int count = 0;
    while(rs.next()) {
      count++;
    }
    assertEquals(count, 1);
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} is registered in the
   * {@link DriverManager}.
   *
   * @throws SQLException If an error occurs. (This is not supposed to happen.)
   */
  @Test
  public void testDriverIsRegisteredInDriverManager() throws Exception {
    assert DriverManager.getDriver(
        "jdbc:arrow-flight://localhost:32010") instanceof ArrowFlightJdbcDriver;
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} fails when provided with an
   * unsupported URL prefix.
   *
   * @throws SQLException If the test passes.
   */
  @Test(expected = SQLException.class)
  public void testShouldDeclineUrlWithUnsupportedPrefix() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    driver.connect("jdbc:mysql://localhost:32010", dataSource.getProperties("flight", "flight123"))
        .close();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can establish a successful
   * connection to the Arrow Flight client.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldConnectWhenProvidedWithValidUrl() throws Exception {
    // Get the Arrow Flight JDBC driver by providing a URL with a valid prefix.
    final Driver driver = new ArrowFlightJdbcDriver();

    try (Connection connection =
             driver.connect("jdbc:arrow-flight://" +
                     dataSource.getConfig().getHost() + ":" +
                     dataSource.getConfig().getPort() + "?" +
                     "useEncryption=false",
        dataSource.getProperties(dataSource.getConfig().getUser(), dataSource.getConfig().getPassword()))) {
      assert connection.isValid(300);
    }
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToMalformedUrl() throws SQLException {
    final Driver driver = new ArrowFlightJdbcDriver();
    final String malformedUri = "yes:??/chainsaw.i=T333";

    driver.connect(malformedUri, dataSource.getProperties("flight", "flight123"));
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoPrefix() throws SQLException {
    final Driver driver = new ArrowFlightJdbcDriver();
    final String malformedUri = "localhost:32010";

    driver.connect(malformedUri, dataSource.getProperties(dataSource.getConfig().getUser(),
        dataSource.getConfig().getPassword()));
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  @Ignore // TODO Rework this test.
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoPort() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    // FIXME This test was passing because the prefix was wrong, NOT because it didn't specify the port.
    final String malformedUri = "jdbc:arrow-flight://32010:localhost";
    driver.connect(malformedUri, dataSource.getProperties("flight", "flight123"));
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  @Ignore // TODO Rework this test.
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoHost() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    // FIXME This test was passing because the prefix was wrong, NOT because it didn't specify the host.
    final String malformedUri = "jdbc:arrow-flight://32010:localhost";
    driver.connect(malformedUri, dataSource.getProperties(dataSource.getConfig().getUser(),
        dataSource.getConfig().getPassword()));
  }

  /**
   * Tests whether {@code ArrowFlightJdbcDriverTest#getUrlsArgs} returns the
   * correct URL parameters.
   *
   * @throws Exception If an error occurs.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDriverUrlParsingMechanismShouldReturnTheDesiredArgsFromUrl() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method parseUrl = driver.getClass().getDeclaredMethod("getUrlsArgs", String.class);

    parseUrl.setAccessible(true);

    final Map<Object, Object> parsedArgs = (Map<Object, Object>) parseUrl.invoke(driver,
        "jdbc:arrow-flight://localhost:2222/?key1=value1&key2=value2&a=b");

    // Check size == the amount of args provided (scheme not included)
    assertEquals(5, parsedArgs.size());

    // Check host == the provided host
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.HOST.camelName()), "localhost");

    // Check port == the provided port
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.PORT.camelName()), 2222);

    // Check all other non-default arguments
    assertEquals(parsedArgs.get("key1"), "value1");
    assertEquals(parsedArgs.get("key2"), "value2");
    assertEquals(parsedArgs.get("a"), "b");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDriverUrlParsingMechanismShouldReturnTheDesiredArgsFromUrlWithSemicolon() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method parseUrl = driver.getClass().getDeclaredMethod("getUrlsArgs", String.class);

    parseUrl.setAccessible(true);

    final Map<Object, Object> parsedArgs = (Map<Object, Object>) parseUrl.invoke(driver,
        "jdbc:arrow-flight://localhost:2222/;key1=value1;key2=value2;a=b");

    // Check size == the amount of args provided (scheme not included)
    assertEquals(5, parsedArgs.size());

    // Check host == the provided host
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.HOST.camelName()), "localhost");

    // Check port == the provided port
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.PORT.camelName()), 2222);

    // Check all other non-default arguments
    assertEquals(parsedArgs.get("key1"), "value1");
    assertEquals(parsedArgs.get("key2"), "value2");
    assertEquals(parsedArgs.get("a"), "b");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDriverUrlParsingMechanismShouldReturnTheDesiredArgsFromUrlWithOneSemicolon() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method parseUrl = driver.getClass().getDeclaredMethod("getUrlsArgs", String.class);

    parseUrl.setAccessible(true);

    final Map<Object, Object> parsedArgs = (Map<Object, Object>) parseUrl.invoke(driver,
        "jdbc:arrow-flight://localhost:2222/;key1=value1");

    // Check size == the amount of args provided (scheme not included)
    assertEquals(3, parsedArgs.size());

    // Check host == the provided host
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.HOST.camelName()), "localhost");

    // Check port == the provided port
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.PORT.camelName()), 2222);

    // Check all other non-default arguments
    assertEquals(parsedArgs.get("key1"), "value1");
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @SuppressWarnings("unchecked")
  @Test(expected = SQLException.class)
  public void testDriverUrlParsingMechanismShouldThrowExceptionUponProvidedWithMalformedUrl()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method getUrlsArgs = driver.getClass().getDeclaredMethod("getUrlsArgs", String.class);

    getUrlsArgs.setAccessible(true);

    try {
      final Map<String, String> parsedArgs = (Map<String, String>) getUrlsArgs.invoke(driver,
          "jdbc:malformed-url-flight://localhost:2222");
    } catch (InvocationTargetException e) {
      throw (SQLException) e.getCause();
    }
  }

  /**
   * Tests whether {@code ArrowFlightJdbcDriverTest#getUrlsArgs} returns the
   * correct URL parameters when the host is an IP Address.
   *
   * @throws Exception If an error occurs.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDriverUrlParsingMechanismShouldWorkWithIPAddress() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method getUrlsArgs = driver.getClass().getDeclaredMethod("getUrlsArgs", String.class);

    getUrlsArgs.setAccessible(true);

    final Map<String, String> parsedArgs =
        (Map<String, String>) getUrlsArgs.invoke(driver, "jdbc:arrow-flight://0.0.0.0:2222");

    // Check size == the amount of args provided (scheme not included)
    assertEquals(2, parsedArgs.size());

    // Check host == the provided host
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.HOST.camelName()), "0.0.0.0");

    // Check port == the provided port
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.PORT.camelName()), 2222);
  }

  /**
   * Tests whether {@code ArrowFlightJdbcDriverTest#getUrlsArgs} escape especial characters and returns the
   * correct URL parameters when the especial character '&' is embedded in the query parameters values.
   *
   * @throws Exception If an error occurs.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDriverUrlParsingMechanismShouldWorkWithEmbeddedEspecialCharacter()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method getUrlsArgs = driver.getClass().getDeclaredMethod("getUrlsArgs", String.class);

    getUrlsArgs.setAccessible(true);

    final Map<String, String> parsedArgs = (Map<String, String>) getUrlsArgs.invoke(driver,
        "jdbc:arrow-flight://0.0.0.0:2222?test1=test1value&test2%26continue=test2value&test3=test3value");

    // Check size == the amount of args provided (scheme not included)
    assertEquals(5, parsedArgs.size());

    // Check host == the provided host
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.HOST.camelName()), "0.0.0.0");

    // Check port == the provided port
    assertEquals(parsedArgs.get(ArrowFlightConnectionProperty.PORT.camelName()), 2222);

    // Check all other non-default arguments
    assertEquals(parsedArgs.get("test1"), "test1value");
    assertEquals(parsedArgs.get("test2&continue"), "test2value");
    assertEquals(parsedArgs.get("test3"), "test3value");
  }

}

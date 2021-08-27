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

package org.apache.arrow.driver.jdbc.test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.driver.jdbc.client.impl.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.test.utils.FlightTestUtils;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Tests for {@link Connection}.
 * TODO Update to use {@link FlightServerTestRule} instead of {@link FlightTestUtils}
 */
public class ConnectionTest {

  private FlightServer server;
  private String serverUrl;
  private BufferAllocator allocator;
  private FlightTestUtils flightTestUtils;

  /**
   * Setup for all tests.
   *
   * @throws ClassNotFoundException If the {@link ArrowFlightJdbcDriver} cannot be loaded.
   */
  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);

    flightTestUtils = new FlightTestUtils("localhost", "flight1", "woho1",
        "invalid", "wrong");

    final FlightProducer flightProducer = flightTestUtils
        .getFlightProducer(allocator);
    this.server = flightTestUtils.getStartedServer(
        location -> FlightServer.builder(allocator, location, flightProducer)
            .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                new BasicCallHeaderAuthenticator(this::validate)))
            .build());
    serverUrl = flightTestUtils.getConnectionPrefix() +
        flightTestUtils.getUrl() + ":" + this.server.getPort();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(server, allocator);
  }

  /**
   * Validate the user's credential on a FlightServer.
   *
   * @param username flight server username.
   * @param password flight server password.
   * @return the result of validation.
   */
  private CallHeaderAuthenticator.AuthResult validate(final String username,
                                                      final String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (flightTestUtils.getUsername1().equals(username) &&
        flightTestUtils.getPassword1().equals(password)) {
      identity = flightTestUtils.getUsername1();
    } else {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Username or password is invalid.")
          .toRuntimeException();
    }
    return () -> identity;
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * the provided valid credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedValidCredentials()
      throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(), server.getPort());
    properties.put(BuiltInConnectionProperty.AVATICA_USER.camelName(), flightTestUtils.getUsername1());
    properties.put(BuiltInConnectionProperty.AVATICA_PASSWORD.camelName(), flightTestUtils.getPassword1());

    try (Connection connection = DriverManager.getConnection(serverUrl, properties)) {
      assert connection.isValid(300);
    }
  }

  /**
   * Checks if the exception SQLException is thrown when trying to establish a connection without a host.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionWithEmptyHost()
      throws Exception {
    final Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    String invalidUrl = flightTestUtils.getConnectionPrefix();

    Exception exception = null;
    try (Connection connection = DriverManager
        .getConnection(invalidUrl, properties)) {
      Assert.fail();
    } catch (final IllegalStateException e) {
      exception = e;
    }
    assertEquals(format("Required property not provided: <%s>.", ArrowFlightConnectionProperty.HOST), exception.getMessage());
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException on error.
   */
  @Test
  public void testGetBasicClientAuthenticatedShouldOpenConnection()
      throws Exception {

    try (FlightClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(flightTestUtils.getUrl())
                 .withPort(server.getPort())
                 .withUsername(flightTestUtils.getUsername1())
                 .withPassword(flightTestUtils.getPassword1())
                 .withBufferAllocator(allocator)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if the exception IllegalArgumentException is thrown when trying to establish an  unencrypted
   * connection providing with an invalid port.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionProvidingInvalidPort()
      throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(BuiltInConnectionProperty.AVATICA_USER.camelName(), flightTestUtils.getUsername1());
    properties.put(BuiltInConnectionProperty.AVATICA_PASSWORD.camelName(), flightTestUtils.getPassword1());
    String invalidUrl = flightTestUtils.getConnectionPrefix() + flightTestUtils.getUrl() + ":" + 65537;

    Exception exception = null;
    try (Connection connection = DriverManager
        .getConnection(invalidUrl, properties)) {
      Assert.fail();
    } catch (final IllegalStateException e) {
      exception = e;
    }
    assertEquals(format("Required property not provided: <%s>.", ArrowFlightConnectionProperty.PORT), exception.getMessage());
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException on error.
   */
  @Test
  public void testGetBasicClientNoAuthShouldOpenConnection() throws Exception {

    try (FlightClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(flightTestUtils.getUrl())
                 .withBufferAllocator(allocator)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * not providing credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWithoutAuthentication()
      throws Exception {
    final Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(), server.getPort());
    try (Connection connection = DriverManager
        .getConnection(serverUrl, properties)) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with
   * invalid credentials.
   *
   * @throws SQLException The exception expected to be thrown.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithInvalidCredentials()
      throws Exception {

    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(), server.getPort());
    properties.put(BuiltInConnectionProperty.AVATICA_USER.camelName(), flightTestUtils.getUsernameInvalid());
    properties.put(BuiltInConnectionProperty.AVATICA_PASSWORD.camelName(), flightTestUtils.getPasswordInvalid());

    try (Connection connection = DriverManager.getConnection(serverUrl, properties)) {
      Assert.fail();
    }
  }
}

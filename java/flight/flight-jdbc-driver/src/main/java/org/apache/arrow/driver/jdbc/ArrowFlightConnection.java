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

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.arrow.driver.jdbc.utils.DefaultProperty;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;

/**
 * Connection to the Arrow Flight server.
 */
public final class ArrowFlightConnection extends AvaticaConnection {

  private BufferAllocator allocator;

  // TODO Use this later to run queries.
  @SuppressWarnings("unused")
  private ArrowFlightClient client;

  /**
   * Instantiates a new Arrow Flight Connection.
   *
   * @param driver The JDBC driver to use.
   * @param factory The Avatica Factory to use.
   * @param url The URL to connect to.
   * @param info The properties of this connection.
   * @throws SQLException If the connection cannot be established.
   */
  public ArrowFlightConnection(ArrowFlightJdbcDriver driver,
      AvaticaFactory factory, String url, Properties info) throws SQLException {
    super(driver, factory, url, info);
    allocator = new RootAllocator(
        Integer.MAX_VALUE);
    
    try {
      loadClient();
    } catch (SQLException e) {
      allocator.close();
      throw e;
    }
  }

  /**
   * Sets {@link #client} based on the properties of this connection.
   *
   * @throws KeyStoreException
   *           If an error occurs while trying to retrieve KeyStore information.
   * @throws NoSuchAlgorithmException
   *           If a particular cryptographic algorithm is required but does not
   *           exist.
   * @throws CertificateException
   *           If an error occurs while trying to retrieve certificate
   *           information.
   * @throws IOException
   *           If an I/O operation fails.
   * @throws NumberFormatException
   *           If the port number to connect to is invalid.
   * @throws URISyntaxException
   *           If the URI syntax is invalid.
   */
  private void loadClient() throws SQLException {

    if (client != null) {
      throw new IllegalStateException("Client already loaded.");
    }

    String host = (String) info.getOrDefault(DefaultProperty.HOST.toString(),
        "localhost");
    Preconditions.checkArgument(!host.trim().isEmpty());

    int port = Integer.parseInt((String) info.getOrDefault(DefaultProperty.PORT
        .toString(), "32010"));
    Preconditions.checkArgument(0 < port && port < 65536);

    @Nullable
    String username = info.getProperty(DefaultProperty.USER.toString());

    @Nullable
    String password = info.getProperty(DefaultProperty.PASS.toString());

    boolean useTls = ((String) info.getOrDefault(DefaultProperty.USE_TLS
        .toString(), "false"))
        .equalsIgnoreCase("true");
    
    boolean authenticate = username != null;

    if (!useTls) {

      if (authenticate) {
        client = ArrowFlightClient.getBasicClientAuthenticated(allocator, host,
            port, username, password, null);
        return;
      }

      client = ArrowFlightClient.getBasicClientNoAuth(allocator, host, port,
          null);
      return;

    }

    String keyStorePath = info.getProperty(
        DefaultProperty.KEYSTORE_PATH.toString());
    String keyStorePass = info.getProperty(
        DefaultProperty.KEYSTORE_PASS.toString());

    if (authenticate) {
      client = ArrowFlightClient.getEncryptedClientAuthenticated(allocator,
          host, port, null, username, password, keyStorePath, keyStorePass);
      return;
    }

    client = ArrowFlightClient.getEncryptedClientNoAuth(allocator, host,
        port, null, keyStorePath, keyStorePass);
  }

  @Override
  public void close() throws SQLException {
    try {
      client.close();
    } catch (Exception e) {
      throw new SQLException(
          "Failed to close the connection " +
              "to the Arrow Flight client.", e);
    }

    try {
      allocator.close();
    } catch (Exception e) {
      throw new SQLException("Failed to close the resource allocator used " +
          "by the Arrow Flight client.", e);
    }

    super.close();
  }

}

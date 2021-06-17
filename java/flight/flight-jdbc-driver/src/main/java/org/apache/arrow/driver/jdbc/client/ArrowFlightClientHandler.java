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

package org.apache.arrow.driver.jdbc.client;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.annotation.Nullable;

import org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.google.common.base.Optional;

/**
 * An adhoc {@link FlightClient} wrapper, used to access the client. Allows for
 * the reuse of credentials and properties.
 */
public class ArrowFlightClientHandler implements FlightClientHandler {

  private final FlightClient client;

  @Nullable
  private CredentialCallOption token;

  @Nullable
  private HeaderCallOption properties;

  protected ArrowFlightClientHandler(final FlightClient client,
      @Nullable final CredentialCallOption token,
      @Nullable final HeaderCallOption properties) {
    this(client, token);
    this.properties = properties;
  }

  protected ArrowFlightClientHandler(final FlightClient client,
      @Nullable final CredentialCallOption token) {
    this(client);
    this.token = token;
  }

  protected ArrowFlightClientHandler(final FlightClient client,
      final HeaderCallOption properties) {
    this(client, null, properties);
  }

  protected ArrowFlightClientHandler(final FlightClient client) {
    this.client = client;
  }

  /**
   * Gets the {@link FlightClient} wrapped by this handler.
   *
   * @return the client wrapped by this.
   */
  protected final FlightClient getClient() {
    return client;
  }

  /**
   * Gets the bearer token for subsequent calls to this client.
   *
   * @return the bearer token, if it exists; otherwise, empty.
   */
  protected final Optional<CredentialCallOption> getBearerToken() {
    return Optional.fromNullable(token);
  }

  /**
   * Gets the headers for subsequent calls to this client.
   *
   * @return the {@link #properties} of this client, if they exist; otherwise,
   *         empty.
   */
  protected final Optional<HeaderCallOption> getProperties() {
    return Optional.fromNullable(properties);
  }

  /**
   * Makes an RPC "getInfo" request with the given query and client properties
   * in order to retrieve the metadata associated with a set of data records.
   *
   * @param query
   *          The query to retrieve FlightInfo for.
   * @return a {@link FlightInfo} object.
   */
  protected FlightInfo getInfo(final String query) {
    return null;
  }

  @Override
  public VectorSchemaRoot runQuery(final String query) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FlightStream getStream(final String query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public final void close() throws Exception {
    try {
      client.close();
    } catch (final InterruptedException e) {
      /*
       * TODO Consider using a proper logger (e.g., Avatica's Log4JLogger.)
       *
       * This portion of the code should probably be concerned about propagating
       * that an Exception has occurred, as opposed to simply "eating it up."
       */
      System.out.println("[WARNING] Failed to close resource.");

      /*
       * FIXME Should we really be doing this?
       *
       * Perhaps a better idea is to throw the aforementioned exception and, if
       * necessary, handle it later.
       */
      e.printStackTrace();
    }
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator
   *          The {@link BufferAllocator}.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param username
   *          The username for authentication, if needed.
   * @param password
   *          The password for authentication, if needed.
   * @param properties
   *          The {@link HeaderCallOption} of this client, if needed.
   * @param keyStorePath
   *          The keystore path for establishing a TLS-encrypted connection, if
   *          needed.
   * @param keyStorePass
   *          The keystore password for establishing a TLS-encrypted connection,
   *          if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the
   *         aforementioned information.
   * @throws GeneralSecurityException
   *           If a certificate-related error occurs.
   * @throws IOException
   *           If an error occurs while trying to establish a connection to the
   *           client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final String username, @Nullable final String password,
      @Nullable final HeaderCallOption properties,
      @Nullable final String keyStorePath, @Nullable final String keyStorePass)
      throws GeneralSecurityException, IOException {

    final FlightClient.Builder builder = FlightClient.builder()
        .allocator(allocator);

    ArrowFlightClientHandler handler;

    DetermineEncryption: {
      /*
       * Check whether to use TLS encryption based upon:
       * "Was the keystore path provided?"
       */
      final boolean useTls = Optional.fromNullable(keyStorePath).isPresent();

      if (!useTls) {

        // Build a secure TLS-encrypted connection.
        builder.location(Location.forGrpcInsecure(host, port));
        break DetermineEncryption;
      }

      // Build a secure TLS-encrypted connection.
      builder.location(Location.forGrpcTls(host, port)).useTls()
          .trustedCertificates(ClientAuthenticationUtils
              .getCertificateStream(keyStorePath, keyStorePass));
    }

    DetermineAuthentication: {

      /*
       * Check whether to use username/password credentials to authenticate to
       * the Flight Client.
       */
      final boolean useAuthentication = Optional.fromNullable(username)
          .isPresent();

      if (!useAuthentication) {

        final FlightClient client = builder.build();

        // Build an unauthenticated client.
        handler = new ArrowFlightClientHandler(client, properties);
        break DetermineAuthentication;
      }

      final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(
          new ClientBearerHeaderHandler());

      builder.intercept(factory);

      final FlightClient client = builder.build();

      // Build an authenticated client.
      handler = new ArrowFlightClientHandler(client, ClientAuthenticationUtils
          .getAuthenticate(client, username, password, factory, properties),
          properties);
    }

    return handler;
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator
   *          The {@link BufferAllocator}.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param username
   *          The username for authentication, if needed.
   * @param password
   *          The password for authentication, if needed.
   * @param properties
   *          The {@link HeaderCallOption} of this client, if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the
   *         aforementioned information.
   * @throws GeneralSecurityException
   *           If a certificate-related error occurs.
   * @throws IOException
   *           If an error occurs while trying to establish a connection to the
   *           client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final String username, @Nullable final String password,
      @Nullable final HeaderCallOption properties)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, username, password, properties,
        null, null);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator
   *          The {@link BufferAllocator}.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param username
   *          The username for authentication, if needed.
   * @param password
   *          The password for authentication, if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the
   *         aforementioned information.
   * @throws GeneralSecurityException
   *           If a certificate-related error occurs.
   * @throws IOException
   *           If an error occurs while trying to establish a connection to the
   *           client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final String username, @Nullable final String password)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, username, password, null);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator
   *          The {@link BufferAllocator}.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @return a new {@link ArrowFlightClientHandler} based upon the
   *         aforementioned information.
   * @throws GeneralSecurityException
   *           If a certificate-related error occurs.
   * @throws IOException
   *           If an error occurs while trying to establish a connection to the
   *           client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, null, null);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param allocator
   *          The {@link BufferAllocator}.
   * @param host
   *          The host to connect to.
   * @param port
   *          The port to connect to.
   * @param properties
   *          The {@link HeaderCallOption} of this client, if needed.
   * @param keyStorePath
   *          The keystore path for establishing a TLS-encrypted connection, if
   *          needed.
   * @param keyStorePass
   *          The keystore password for establishing a TLS-encrypted connection,
   *          if needed.
   * @return a new {@link ArrowFlightClientHandler} based upon the
   *         aforementioned information.
   * @throws GeneralSecurityException
   *           If a certificate-related error occurs.
   * @throws IOException
   *           If an error occurs while trying to establish a connection to the
   *           client.
   */
  public static final ArrowFlightClientHandler getClient(
      final BufferAllocator allocator, final String host, final int port,
      @Nullable final HeaderCallOption properties,
      @Nullable final String keyStorePath, @Nullable final String keyStorePass)
      throws GeneralSecurityException, IOException {

    return getClient(allocator, host, port, null, null, properties,
        keyStorePath, keyStorePass);
  }
}

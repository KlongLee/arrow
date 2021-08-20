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

package org.apache.arrow.driver.jdbc.client.impl;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.client.BareFlightClientHandler;
import org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils;
import org.apache.arrow.driver.jdbc.client.utils.ClientCreationUtils;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * An adhoc {@link FlightClient} wrapper, used to access the client. Allows for
 * the reuse of credentials and properties.
 */
public class BareArrowFlightClientHandler extends ArrowFlightClientHandler implements BareFlightClientHandler {
  private final FlightClient client;

  protected BareArrowFlightClientHandler(final FlightClient client, final CallOption... options) {
    super(options);
    this.client = Preconditions.checkNotNull(client);
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param address      the host and port to use.
   * @param credentials  the username and password to use.
   * @param keyStoreInfo the KeyStore path and password to use.
   * @param allocator    the {@link BufferAllocator}.
   * @param useTls       whether to use TLS encryption.
   * @param options      the options.
   * @return a new {@link BareArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static BareArrowFlightClientHandler createNewHandler(final Entry<String, Integer> address,
                                                              final Entry<String, String> credentials,
                                                              final Entry<String, String> keyStoreInfo,
                                                              final BufferAllocator allocator,
                                                              final boolean useTls,
                                                              final CallOption... options)
      throws GeneralSecurityException, IOException {
    return createNewHandler(address, credentials, keyStoreInfo, allocator, useTls, Arrays.asList(options));
  }

  /**
   * Gets a new client based upon provided info.
   *
   * @param address      the host and port to use.
   * @param credentials  the username and password to use.
   * @param keyStoreInfo the KeyStore path and password to use.
   * @param allocator    the {@link BufferAllocator}.
   * @param useTls       whether to use TLS encryption.
   * @param options      the options.
   * @return a new {@link BareArrowFlightClientHandler} based upon the aforementioned information.
   * @throws GeneralSecurityException If a certificate-related error occurs.
   * @throws IOException              If an error occurs while trying to establish a connection to the
   *                                  client.
   */
  public static BareArrowFlightClientHandler createNewHandler(final Entry<String, Integer> address,
                                                              final Entry<String, String> credentials,
                                                              final Entry<String, String> keyStoreInfo,
                                                              final BufferAllocator allocator,
                                                              final boolean useTls,
                                                              final Collection<CallOption> options)
      throws GeneralSecurityException, IOException {
    final Entry<FlightClient, List<CallOption>> clientInfo =
            ClientCreationUtils.createAndGetClientInfo(
                    address, credentials, keyStoreInfo,
                    allocator, useTls, options);
    final FlightClient client = clientInfo.getKey();
    final List<CallOption> theseOptions = clientInfo.getValue();
    return new BareArrowFlightClientHandler(client, theseOptions.toArray(new CallOption[0]));
  }


  /**
   * Gets the {@link FlightClient} wrapped by this handler.
   *
   * @return the client wrapped by this.
   */
  public final FlightClient getClient() {
    return client;
  }
}

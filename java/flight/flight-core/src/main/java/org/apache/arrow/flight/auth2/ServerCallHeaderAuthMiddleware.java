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

package org.apache.arrow.flight.auth2;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * Middleware that's used to validate credentials during the handshake and verify
 * the bearer token in subsequent requests.
 */
public class ServerCallHeaderAuthMiddleware implements FlightServerMiddleware {
  /**
   * Factory for accessing ServerAuthMiddleware.
   */
  public static class Factory implements FlightServerMiddleware.Factory<ServerCallHeaderAuthMiddleware> {
    private final CallHeaderAuthenticator authHandler;
    private final GeneratedBearerTokenAuthHandler bearerTokenAuthHandler;

    /**
     * Construct a factory with the given auth handler.
     * @param authHandler The auth handler what will be used for authenticating requests.
     */
    public Factory(CallHeaderAuthenticator authHandler) {
      this.authHandler = authHandler;
      bearerTokenAuthHandler = authHandler.enableCachedCredentials() ?
          new GeneratedBearerTokenAuthHandler() : null;
    }

    @Override
    public ServerCallHeaderAuthMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders,
                                                        RequestContext context) {
      // Check if bearer token auth is being used, and if we've enabled use of server-generated
      // bearer tokens.
      if (authHandler.enableCachedCredentials()) {
        final String bearerTokenFromHeaders =
            AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
        if (bearerTokenFromHeaders != null) {
          final CallHeaderAuthenticator.AuthResult result = bearerTokenAuthHandler.authenticate(incomingHeaders);
          context.put(Auth2Constants.PEER_IDENTITY_KEY, result.getPeerIdentity());
          return new ServerCallHeaderAuthMiddleware(result.getBearerToken().get());
        }
      }

      // Delegate to server auth handler to do the validation.
      final CallHeaderAuthenticator.AuthResult result = authHandler.authenticate(incomingHeaders);
      final String bearerToken;
      if (authHandler.enableCachedCredentials()) {
        bearerToken = bearerTokenAuthHandler.registerBearer(result);
      } else {
        bearerToken = result.getBearerToken().get();
      }
      context.put(Auth2Constants.PEER_IDENTITY_KEY, result.getPeerIdentity());
      return new ServerCallHeaderAuthMiddleware(bearerToken);
    }
  }

  private final String bearerToken;

  public ServerCallHeaderAuthMiddleware(String bearerToken) {
    this.bearerToken = bearerToken;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    if (bearerToken != null &&
        null == AuthUtilities.getValueFromAuthHeader(outgoingHeaders, Auth2Constants.BEARER_PREFIX)) {
      outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
    }
  }

  @Override
  public void onCallCompleted(CallStatus status) {
  }

  @Override
  public void onCallErrored(Throwable err) {
  }
}

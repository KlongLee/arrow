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

package org.apache.arrow.flight;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.Timestamp;

/**
 * POJO to convert to/from the underlying protobuf FlightEndpoint.
 */
public class FlightEndpoint {
  private final List<Location> locations;
  private final Ticket ticket;
  private final Instant expirationTime;
  private final String appMetadata;

  /**
   * Constructs a new endpoint with no expiration time.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public FlightEndpoint(Ticket ticket, Location... locations) {
    this(ticket, /*expirationTime*/null, locations);
  }

  /**
   * Constructs a new endpoint with an expiration time.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param expirationTime (optional) When this endpoint expires.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public FlightEndpoint(Ticket ticket, Instant expirationTime, Location... locations) {
    this(ticket, expirationTime, null, locations);
  }

  /**
   * Constructs a new endpoint with an expiration time.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param expirationTime (optional) When this endpoint expires.
   * @param appMetadata (optional) Application metadata associated with this endpoint.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public FlightEndpoint(Ticket ticket, Instant expirationTime, String appMetadata, Location... locations) {
    Objects.requireNonNull(ticket);
    this.locations = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(locations)));
    this.expirationTime = expirationTime;
    this.ticket = ticket;
    this.appMetadata = appMetadata;
  }

  /**
   * Constructs from the protocol buffer representation.
   */
  FlightEndpoint(Flight.FlightEndpoint flt) throws URISyntaxException {
    this.locations = new ArrayList<>();
    for (final Flight.Location location : flt.getLocationList()) {
      this.locations.add(new Location(location.getUri()));
    }
    if (flt.hasExpirationTime()) {
      this.expirationTime = Instant.ofEpochSecond(
          flt.getExpirationTime().getSeconds(), flt.getExpirationTime().getNanos());
    } else {
      this.expirationTime = null;
    }
    this.appMetadata = flt.getAppMetadata().toStringUtf8();
    this.ticket = new Ticket(flt.getTicket());
  }

  public List<Location> getLocations() {
    return locations;
  }

  public Ticket getTicket() {
    return ticket;
  }

  public Optional<Instant> getExpirationTime() {
    return Optional.ofNullable(expirationTime);
  }

  public String getAppMetadata() {
    return appMetadata;
  }

  /**
   * Converts to the protocol buffer representation.
   */
  Flight.FlightEndpoint toProtocol() {
    Flight.FlightEndpoint.Builder b = Flight.FlightEndpoint.newBuilder()
        .setTicket(ticket.toProtocol())
        .setAppMetadata(ByteString.copyFromUtf8(appMetadata));

    for (Location l : locations) {
      b.addLocation(l.toProtocol());
    }

    if (expirationTime != null) {
      b.setExpirationTime(
          Timestamp.newBuilder()
              .setSeconds(expirationTime.getEpochSecond())
              .setNanos(expirationTime.getNano())
              .build());
    }

    return b.build();
  }

  /**
   * Get the serialized form of this protocol message.
   *
   * <p>Intended to help interoperability by allowing non-Flight services to still return Flight types.
   */
  public ByteBuffer serialize() {
    return ByteBuffer.wrap(toProtocol().toByteArray());
  }

  /**
   * Parse the serialized form of this protocol message.
   *
   * <p>Intended to help interoperability by allowing Flight clients to obtain stream info from non-Flight services.
   *
   * @param serialized The serialized form of the message, as returned by {@link #serialize()}.
   * @return The deserialized message.
   * @throws IOException if the serialized form is invalid.
   * @throws URISyntaxException if the serialized form contains an unsupported URI format.
   */
  public static FlightEndpoint deserialize(ByteBuffer serialized) throws IOException, URISyntaxException {
    return new FlightEndpoint(Flight.FlightEndpoint.parseFrom(serialized));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlightEndpoint that = (FlightEndpoint) o;
    return locations.equals(that.locations) &&
        ticket.equals(that.ticket) &&
        Objects.equals(expirationTime, that.expirationTime) &&
        Objects.equals(appMetadata, that.appMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(locations, ticket, expirationTime, appMetadata);
  }

  @Override
  public String toString() {
    return "FlightEndpoint{" +
        "locations=" + locations +
        ", ticket=" + ticket +
        ", expirationTime=" + (expirationTime == null ? "(none)" : expirationTime.toString()) +
        ", appMetadata=" + appMetadata +
        '}';
  }
}

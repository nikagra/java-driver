/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class AddressUtils {

  public static Set<InetSocketAddress> extract(String address, boolean resolve) {
    int separator = address.lastIndexOf(':');
    if (separator < 0) {
      throw new IllegalArgumentException("expecting format host:port");
    }

    String host = address.substring(0, separator);
    String portString = address.substring(separator + 1);
    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("expecting port to be a number, got " + portString, e);
    }
    if (!resolve) {
      return ImmutableSet.of(InetSocketAddress.createUnresolved(host, port));
    } else {
      InetAddress[] inetAddresses;
      try {
        inetAddresses = InetAddress.getAllByName(host);
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
      Set<InetSocketAddress> result = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        result.add(new InetSocketAddress(inetAddress, port));
      }
      return result;
    }
  }

  /**
   * Returns a copy of {@code ip} labelled with {@code hostName}, or unlabelled when {@code
   * hostName} is {@code null}, preserving an IPv6 scope id if there is one. Performs no lookup.
   *
   * <p>The scoped {@link Inet6Address#getByAddress(String, byte[], int)} overload is used only when
   * the address really has a scope: {@code Inet6AddressHolder.init} treats any {@code scope_id >=
   * 0} as scoped, so passing the {@code 0} that an unscoped address reports would append a spurious
   * {@code %0} to {@code getHostAddress()} (verified on JDK 11.0.30), and that would reach metric
   * tags through an endpoint's {@code toString()}.
   */
  public static InetAddress withHostName(@Nullable String hostName, InetAddress ip)
      throws UnknownHostException {
    if (ip instanceof Inet6Address) {
      int scopeId = ((Inet6Address) ip).getScopeId();
      if (scopeId != 0) {
        return Inet6Address.getByAddress(hostName, ip.getAddress(), scopeId);
      }
    }
    return InetAddress.getByAddress(hostName, ip.getAddress());
  }

  /**
   * Returns {@code address} with its host-name label removed, so that {@code getHostString()} and
   * {@code toString()} report the IP literal and cannot start reporting something else later, or
   * {@code null} if {@code address} carries no {@link InetAddress} to strip. Performs no lookup.
   *
   * <p>Anything deriving a durable identity from a resolved address must do this first: the label
   * renders a mutable field of the shared {@code InetAddress}, filled in by the first {@code
   * getHostName()} call (a reverse lookup that {@code DefaultSslEngineFactory} performs under the
   * default {@code allow-dns-reverse-lookup-san}), so an identity keyed off it would move the first
   * time the node is connected to over TLS.
   */
  @Nullable
  public static InetSocketAddress stripHostName(InetSocketAddress address) {
    InetAddress ip = address.getAddress();
    if (ip == null) {
      return null;
    }
    try {
      return new InetSocketAddress(withHostName(null, ip), address.getPort());
    } catch (UnknownHostException impossible) {
      // getByAddress only rejects illegal byte lengths, and these bytes come from a real
      // InetAddress.
      return null;
    }
  }
}

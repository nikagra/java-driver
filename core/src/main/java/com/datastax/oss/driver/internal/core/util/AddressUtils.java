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
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class AddressUtils {

  /** Digits and dots only: every shorthand dotted-decimal form {@code getByName} parses. */
  private static final Pattern NUMERIC = Pattern.compile("[0-9.]+");

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
   * Returns {@code address} with its host-name label removed, so that it reports the IP literal
   * rather than whatever name the resolver happened to hand back, or {@code null} if {@code
   * address} carries no {@link InetAddress} to strip. Performs no lookup.
   *
   * <p>Anything deriving a durable identity from a resolved address must do this first, because the
   * resolver's label is not a property of the node: two contact-point names for the same address
   * would otherwise identify it two different ways. What this buys is an identity fixed at the
   * moment it is taken -- {@code DefaultEndPoint} reads {@code getHostString()} once, in its
   * constructor, and keeps the result as its metric prefix.
   *
   * <p>It does not make the address permanently self-describing, and nothing here can: the label is
   * a mutable field of the {@code InetAddress}, and the first {@code getHostName()} call on it
   * fills that field in with a reverse lookup -- one that {@code DefaultSslEngineFactory} performs
   * under the default {@code allow-dns-reverse-lookup-san}. From then on {@code getHostString()}
   * and {@code toString()} report the PTR name, on this instance and on every {@code InetAddress}
   * the driver builds from raw bytes (every peer's included).
   */
  @Nullable
  public static InetSocketAddress stripHostName(InetSocketAddress address) {
    InetAddress ip = address.getAddress();
    if (ip == null) {
      return null;
    }
    try {
      return new InetSocketAddress(unlabelled(ip), address.getPort());
    } catch (UnknownHostException impossible) {
      // getByAddress only rejects illegal byte lengths, and these bytes come from a real
      // InetAddress.
      return null;
    }
  }

  /**
   * An unlabelled copy of {@code ip}, preserving an IPv6 scope id if there is one.
   *
   * <p>The scoped {@link Inet6Address#getByAddress(String, byte[], int)} overload is used only when
   * the address really has a scope: {@code Inet6AddressHolder.init} treats any {@code scope_id >=
   * 0} as scoped, so passing the {@code 0} that an unscoped address reports would append a spurious
   * {@code %0} to {@code getHostAddress()} (verified on JDK 11.0.30), and that would reach metric
   * tags through an endpoint's {@code toString()}.
   */
  private static InetAddress unlabelled(InetAddress ip) throws UnknownHostException {
    if (ip instanceof Inet6Address) {
      int scopeId = ((Inet6Address) ip).getScopeId();
      if (scopeId != 0) {
        return Inet6Address.getByAddress(null, ip.getAddress(), scopeId);
      }
    }
    return InetAddress.getByAddress(null, ip.getAddress());
  }
  /**
   * Whether this host string is an IP literal rather than a name, decided lexically -- no lookup.
   *
   * <p>Answers the question {@link InetAddress#getByName} answers by parsing: a literal is handed
   * straight back, so an address that holds one will never resolve to anything else. Callers use
   * that to tell an address that can change from one that cannot, so a wrong {@code false} is the
   * costly direction and the check is deliberately generous.
   *
   * <p>Guava's {@code isInetAddress} is the core of it, with two additions it does not cover:
   * brackets, which {@code getByName} accepts around an IPv6 literal; and the shorthand
   * dotted-decimal forms, which it parses numerically ({@code 127.1} is {@code 127.0.0.1}, {@code
   * 10.1.2} is {@code 10.1.0.2}, {@code 1} is {@code 0.0.0.1}) while Guava requires all four parts.
   * Anything made only of digits and dots is therefore treated as a literal, which is the generous
   * reading rather than the exact one: a digits-and-dots form {@code getByName} cannot parse
   * ({@code 1.2.3.4.5}, five parts) is passed to the resolver rather than rejected, so the JDK does
   * treat it as a name -- one whose rightmost label is all digits, which nothing in the public DNS
   * answers. Calling it a literal errs in the safe direction, per the paragraph above.
   *
   * <p>An IPv6 scope id needs no addition: the Guava this build shades accepts {@code fe80::1%3}
   * and {@code fe80::1%eth0}, bracketed or not. Older versions rejected the {@code %} suffix, so
   * {@code AddressUtilsTest} pins those forms rather than trusting the dependency to keep them.
   */
  public static boolean isIpLiteral(String hostname) {
    String host = hostname;
    if (host.length() > 1 && host.charAt(0) == '[' && host.charAt(host.length() - 1) == ']') {
      host = host.substring(1, host.length() - 1);
    }
    return InetAddresses.isInetAddress(host) || NUMERIC.matcher(host).matches();
  }
}

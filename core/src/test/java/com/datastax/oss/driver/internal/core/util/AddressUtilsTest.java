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

import static com.datastax.oss.driver.Assertions.assertThat;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

/**
 * Note: every assertion here reads a label that is already set, or an address literal. Nothing
 * calls {@code getHostName()} on an unlabelled address, which would be a reverse lookup.
 */
public class AddressUtilsTest {

  private static final byte[] IPV4 = {10, 0, 0, 2};
  // 2001:db8::1
  private static final byte[] IPV6 = {
    0x20, 0x01, 0x0d, (byte) 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
  };

  @Test
  public void should_strip_host_name_from_unscoped_ipv6_address() throws Exception {
    Inet6Address unscoped = (Inet6Address) InetAddress.getByAddress("cluster.example.com", IPV6);
    assertThat(unscoped.getScopeId()).isZero();

    InetSocketAddress stripped = AddressUtils.stripHostName(new InetSocketAddress(unscoped, 9042));

    // The whole point of the scopeId != 0 guard: the scoped getByAddress() overload treats any
    // scope_id >= 0 as scoped, so going through it here would report 2001:db8::1%0.
    assertThat(stripped).isNotNull();
    assertThat(stripped.getHostString()).isEqualTo("2001:db8:0:0:0:0:0:1");
    assertThat(stripped.getHostString()).doesNotContain("%");
  }

  @Test
  public void should_strip_host_name_from_resolved_address() throws Exception {
    InetSocketAddress labelled =
        new InetSocketAddress(InetAddress.getByAddress("cluster.example.com", IPV4), 9042);
    assertThat(labelled.getHostString()).isEqualTo("cluster.example.com");

    InetSocketAddress stripped = AddressUtils.stripHostName(labelled);

    assertThat(stripped).isNotNull();
    assertThat(stripped.isUnresolved()).isFalse();
    assertThat(stripped.getPort()).isEqualTo(9042);
    // Bytes and port only: this is what a node's durable identity and its metric prefix key on.
    assertThat(stripped.getHostString()).isEqualTo("10.0.0.2");
    assertThat(stripped.toString()).isEqualTo("/10.0.0.2:9042");
  }

  @Test
  public void should_strip_host_name_and_keep_ipv6_scope_id() throws Exception {
    InetSocketAddress labelled =
        new InetSocketAddress(Inet6Address.getByAddress("cluster.example.com", IPV6, 5), 9042);

    InetSocketAddress stripped = AddressUtils.stripHostName(labelled);

    assertThat(stripped).isNotNull();
    assertThat(stripped.getHostString()).isEqualTo("2001:db8:0:0:0:0:0:1%5");
  }

  @Test
  public void should_not_strip_host_name_from_unresolved_address() {
    InetSocketAddress unresolved = InetSocketAddress.createUnresolved("cluster.example.com", 9042);

    // No InetAddress to strip, so there is no identity to derive: callers keep what they had.
    assertThat(AddressUtils.stripHostName(unresolved)).isNull();
  }

  @Test
  public void should_recognise_ip_literals() {
    assertThat(AddressUtils.isIpLiteral("10.0.0.2")).isTrue();
    assertThat(AddressUtils.isIpLiteral("2001:db8::1")).isTrue();
    // getByName() accepts the bracketed form; Guava's isInetAddress does not.
    assertThat(AddressUtils.isIpLiteral("[2001:db8::1]")).isTrue();
    // Shorthand dotted-decimal: getByName() parses these numerically (127.1 is 127.0.0.1,
    // 10.1.2 is 10.1.0.2, and a bare integer is the whole 32 bits), so they resolve to the same
    // address forever. Guava requires all four parts and calls each of them a name.
    assertThat(AddressUtils.isIpLiteral("127.1")).isTrue();
    assertThat(AddressUtils.isIpLiteral("10.1.2")).isTrue();
    assertThat(AddressUtils.isIpLiteral("2130706433")).isTrue();
    // Five parts is more than getByName() can parse, so it goes to the resolver instead of being
    // rejected -- the one place the widened rule is generous rather than exact. Pinned because it
    // is the safe direction: a name mistaken for a literal only keeps the contact-point fallback.
    assertThat(AddressUtils.isIpLiteral("1.2.3.4.5")).isTrue();
    // Scope ids: accepted by the Guava this build shades, and by getByName() for a numeric zone.
    // Older Guava rejected the % suffix, and a literal read as a name suppresses the one fallback
    // a fixed address has, so pin the forms rather than trust the dependency.
    assertThat(AddressUtils.isIpLiteral("fe80::1%3")).isTrue();
    assertThat(AddressUtils.isIpLiteral("fe80::1%eth0")).isTrue();
    assertThat(AddressUtils.isIpLiteral("[fe80::1%3]")).isTrue();
  }

  @Test
  public void should_recognise_host_names() {
    assertThat(AddressUtils.isIpLiteral("cluster.example.com")).isFalse();
    assertThat(AddressUtils.isIpLiteral("nlb1.example.com")).isFalse();
    // Digits and dots is the widened rule, so a name has to contain something else -- which every
    // name does, since a top-level domain cannot be all-numeric.
    assertThat(AddressUtils.isIpLiteral("10.0.0.2.example.com")).isFalse();
  }
}

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
package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

/**
 * Covers {@link ByteBufCompressor} and both built-in implementations. {@link
 * BuiltInCompressorsTest} only covers the factory lookup, so nothing exercised the compression
 * itself.
 */
@RunWith(DataProviderRunner.class)
public class ByteBufCompressorTest {

  /** Repetitive on purpose, so both algorithms actually shrink it. */
  private static final byte[] PAYLOAD = payload();

  /** Released from {@link #releaseBuffers()} so a failing assertion cannot leak a direct buffer. */
  private TrackingAllocator allocator;

  @Before
  public void setup() {
    allocator = new TrackingAllocator();
  }

  @After
  public void releaseBuffers() {
    allocator.releaseAll();
  }

  private static byte[] payload() {
    byte[] bytes = new byte[512];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i % 8);
    }
    return bytes;
  }

  @DataProvider
  public static Object[][] compressors() {
    return new Object[][] {
      {"snappy", new SnappyCompressor(Mockito.mock(DriverContext.class))},
      {"lz4", new Lz4Compressor("test")},
    };
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_heap_buffer(String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compress(input);
    assertThat(compressed.isDirect()).isFalse();
    // PAYLOAD is repetitive, so a compressor that returned an uncompressed copy would fail here
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    ByteBuf decompressed = compressor.decompress(compressed);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_direct_buffer(String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.directBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compress(input);
    assertThat(compressed.isDirect()).isTrue();
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    ByteBuf decompressed = compressor.decompress(compressed);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_heap_buffer_without_length(
      String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compressWithoutLength(input);
    ByteBuf decompressed = compressor.decompressWithoutLength(compressed, PAYLOAD.length);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_direct_buffer_without_length(
      String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.directBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compressWithoutLength(input);
    ByteBuf decompressed = compressor.decompressWithoutLength(compressed, PAYLOAD.length);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_consume_the_whole_input(String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);

    compressor.compress(input);

    // Every implementation advances the reader index to the writer index
    assertThat(input.readableBytes()).isZero();
  }

  @Test
  @UseDataProvider("compressors")
  public void should_report_its_algorithm(String name, ByteBufCompressor compressor) {
    assertThat(compressor.algorithm()).isEqualTo(name);
  }

  /**
   * LZ4 writes the uncompressed length ahead of the frame and reads it back; Snappy does not, and
   * returns a bogus length that its decompress path ignores.
   */
  @Test
  public void should_prepend_the_uncompressed_length_for_lz4_only() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));

    ByteBuf lz4WithLength = lz4.compress(allocator.heapBuffer().writeBytes(PAYLOAD));
    ByteBuf lz4WithoutLength =
        lz4.compressWithoutLength(allocator.heapBuffer().writeBytes(PAYLOAD));
    ByteBuf snappyWithLength = snappy.compress(allocator.heapBuffer().writeBytes(PAYLOAD));
    ByteBuf snappyWithoutLength =
        snappy.compressWithoutLength(allocator.heapBuffer().writeBytes(PAYLOAD));

    assertThat(lz4WithLength.readableBytes()).isEqualTo(lz4WithoutLength.readableBytes() + 4);
    assertThat(lz4WithLength.getInt(lz4WithLength.readerIndex())).isEqualTo(PAYLOAD.length);
    assertThat(lz4.readUncompressedLength(lz4WithLength)).isEqualTo(PAYLOAD.length);

    assertThat(snappyWithLength.readableBytes()).isEqualTo(snappyWithoutLength.readableBytes());
    assertThat(snappy.readUncompressedLength(snappyWithLength)).isEqualTo(-1);
  }

  @Test
  public void should_reject_a_heap_frame_that_is_not_snappy() {
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));
    ByteBuf garbage = allocator.heapBuffer().writeBytes(notSnappy());

    assertThatThrownBy(() -> snappy.decompress(garbage))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not appear to be Snappy compressed");
  }

  @Test
  public void should_reject_a_direct_frame_that_is_not_snappy() {
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));
    ByteBuf garbage = allocator.directBuffer().writeBytes(notSnappy());

    assertThatThrownBy(() -> snappy.decompress(garbage))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not appear to be Snappy compressed");
  }

  /**
   * Trailing bytes make the frame longer than what LZ4 actually reads, which is the mismatch the
   * decompress path guards against. Also proves the output buffer is released on that path rather
   * than leaked.
   */
  @Test
  public void should_reject_a_heap_frame_whose_length_does_not_match() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBuf compressed = lz4.compressWithoutLength(allocator.heapBuffer().writeBytes(PAYLOAD));
    compressed.writeBytes(new byte[] {1, 2, 3, 4});

    assertThatThrownBy(() -> lz4.decompressWithoutLength(compressed, PAYLOAD.length))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Compressed lengths mismatch");

    assertThat(allocator.lastAllocated().refCnt()).isZero();
  }

  @Test
  public void should_reject_a_direct_frame_whose_length_does_not_match() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBuf compressed = lz4.compressWithoutLength(allocator.directBuffer().writeBytes(PAYLOAD));
    compressed.writeBytes(new byte[] {1, 2, 3, 4});

    assertThatThrownBy(() -> lz4.decompressWithoutLength(compressed, PAYLOAD.length))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Compressed lengths mismatch");

    assertThat(allocator.lastAllocated().refCnt()).isZero();
  }

  private static byte[] notSnappy() {
    byte[] bytes = new byte[64];
    Arrays.fill(bytes, (byte) 0xFF);
    return bytes;
  }

  /**
   * Hands out the buffers the compressors allocate internally, so a test can assert one was
   * released, and cleans up everything afterwards.
   */
  private static class TrackingAllocator extends AbstractByteBufAllocator {

    private final List<ByteBuf> allocated = new ArrayList<>();

    TrackingAllocator() {
      super(false);
    }

    // Pass `this` as the allocator so buf.alloc() leads back here: the compressors allocate their
    // output from the input buffer's own allocator, which is what makes them observable.
    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
      return record(new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
      return record(new UnpooledDirectByteBuf(this, initialCapacity, maxCapacity));
    }

    @Override
    public boolean isDirectBufferPooled() {
      return false;
    }

    private ByteBuf record(ByteBuf buf) {
      allocated.add(buf);
      return buf;
    }

    ByteBuf lastAllocated() {
      return allocated.get(allocated.size() - 1);
    }

    void releaseAll() {
      for (ByteBuf buf : allocated) {
        if (buf.refCnt() > 0) {
          buf.release();
        }
      }
    }
  }
}

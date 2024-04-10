/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.appulse.encon.connection.handshake.message;

import io.appulse.encon.connection.handshake.message.StatusMessage.Status;
import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.val;

import java.util.stream.Stream;

import static io.appulse.encon.connection.handshake.message.MessageType.STATUS;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Locale.ENGLISH;
import static lombok.AccessLevel.PRIVATE;

/**
 *
 * @since 1.0.0
 * @author Artem Labazin
 */
@Getter
@ToString
@FieldDefaults(level = PRIVATE)
@EqualsAndHashCode(callSuper = true)
public class NamedStatusMessage extends Message {

  Status status;
  short nLen;
  String fullName;
  int creation;

  public NamedStatusMessage() {
    super(STATUS);
  }

  @Builder
  private NamedStatusMessage(@NonNull Status status, short nLen, @NonNull String fullName, int creation) {
    this();
    this.status = status;
    this.nLen = nLen;
    this.fullName = fullName;
    this.creation = creation;
  }

  @Override
  void write (ByteBuf buffer) {
    buffer.writeCharSequence(status.getName(), ISO_8859_1);
    buffer.writeShort(((short) fullName.getBytes(ISO_8859_1).length));
    buffer.writeCharSequence(fullName, ISO_8859_1);
    buffer.writeInt(creation);
  }

  @Override
  void read (ByteBuf buffer) {
    status = Status.of(buffer.readCharSequence(buffer.readableBytes(), ISO_8859_1).toString());
    nLen = buffer.readShort();
    fullName = buffer.readCharSequence(buffer.readableBytes(), ISO_8859_1).toString();
    creation = buffer.readInt();
  }
}

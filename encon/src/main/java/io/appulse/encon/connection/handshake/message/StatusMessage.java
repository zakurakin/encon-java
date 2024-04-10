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

import static io.appulse.encon.connection.handshake.message.MessageType.STATUS;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static lombok.AccessLevel.PRIVATE;

import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.val;

/**
 *
 * @since 1.0.0
 * @author Artem Labazin
 */
@Getter
@ToString
@FieldDefaults(level = PRIVATE)
@EqualsAndHashCode(callSuper = true)
public class StatusMessage extends Message {

  Status status;

  public StatusMessage () {
    super(STATUS);
  }

  @Builder
  protected StatusMessage (@NonNull Status status) {
    this();
    this.status = status;
  }

  @Override
  void write (ByteBuf buffer) {
    val statusName = status.getName();
    buffer.writeCharSequence(statusName, ISO_8859_1);
  }

  @Override
  void read (ByteBuf buffer) {
    val string = buffer.readCharSequence(buffer.readableBytes(), ISO_8859_1).toString();
    status = Status.of(string);
  }

  public enum Status {

    /**
     * The handshake will continue.
     */
    OK("ok"),
    /**
     * The handshake will continue, but A is informed that B has another ongoing connection attempt
     * that will be shut down (simultaneous connect where A's name is greater than B's name, compared literally).
     */
    OK_SIMULTANEOUS("ok_simultaneous"),
    /**
     * The handshake will not continue, as B already has an ongoing handshake,
     * which it itself has initiated (simultaneous connect where B's name is greater than A's).
     */
    NOK("nok"),
    /**
     * The connection is disallowed for some (unspecified) security reason.
     */
    NOT_ALLOWED("not_allowed"),
    /**
     * A connection to the node is already active, which either means that node A is confused or
     * that the TCP connection breakdown of a previous node with this name has not yet reached node B.
     */
    ALIVE("alive"),
    /**
     * The handshake will continue, but A requested a dynamic node name by setting flag DFLAG_NAME_ME.
     * The dynamic node name of A is supplied at the end of the status message from B. The host name of A
     * which was sent as Name in send_name will be used by node B to generate the full dynamic node name.
     */
    NAMED("named:"),
    /**
     * Unknown status.
     */
    UNDEFINED("undefined");

    @Getter
    final String name;

    Status(String name) {
        this.name = name;
    }

    public static Status of (@NonNull String string) {
      return Stream.of(values())
          .filter(it -> it.getName().equalsIgnoreCase(string))
          .findAny()
          .orElse(UNDEFINED);
    }
  }
}

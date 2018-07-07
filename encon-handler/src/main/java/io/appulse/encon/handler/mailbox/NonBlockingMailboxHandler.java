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

package io.appulse.encon.handler.mailbox;

import static io.appulse.encon.mailbox.MailboxQueueType.NON_BLOCKING;
import static lombok.AccessLevel.PRIVATE;

import io.appulse.encon.connection.regular.Message;
import io.appulse.encon.handler.message.MessageHandler;
import io.appulse.encon.mailbox.Mailbox;

import lombok.Builder;
import lombok.experimental.FieldDefaults;

/**
 * {@link AbstractMailboxHandler} implementation for non-blocking queue mailboxes.
 *
 * @since 1.4.0
 * @author alabazin
 */
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class NonBlockingMailboxHandler extends AbstractMailboxHandler {

  Mailbox mailbox;

  /**
   * Constructor.
   *
   * @param messageHandler received messages handler
   *
   * @param mailbox mailbox
   */
  @Builder
  public NonBlockingMailboxHandler (MessageHandler messageHandler,
                                    Mailbox mailbox
  ) {
    super(messageHandler, mailbox);
    this.mailbox = mailbox;

    if (mailbox.getQueueType() != NON_BLOCKING) {
      throw new IllegalArgumentException("Non-blocking mailbox handler works only with non-blocking queues");
    }
  }

  @Override
  protected Message getMessage () {
    while (Thread.interrupted()) {
      Message message = mailbox.receive();
      if (message != null) {
        return message;
      }
    }
    return null;
  }
}
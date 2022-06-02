/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.eventhandling.deadletter;


import org.axonframework.messaging.deadletter.QueueIdentifier;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Implementation of the {@link QueueIdentifier} dedicated for dead-lettering in event handling components.
 * <p>
 * This identifier is used to uniquely identify a sequence of events for a specific {@code processingGroup}. The
 * sequence identifier is typically the result of a
 * {@link org.axonframework.eventhandling.async.SequencingPolicy#getSequenceIdentifierFor(Object)} operation.
 *
 * @author Steven van Beelen
 * @see DeadLetteringEventHandlerInvoker
 * @since 4.6.0
 */
public class EventHandlingQueueIdentifier implements QueueIdentifier {

    private final Object sequenceIdentifier;
    private final String processingGroup;

    /**
     * Constructs an event handling specific {@link QueueIdentifier}.
     *
     * @param sequenceIdentifier The identifier of a sequence of events to enqueue.
     * @param processingGroup    The processing group that is required to enqueue events.
     */
    public EventHandlingQueueIdentifier(@Nonnull Object sequenceIdentifier, @Nonnull String processingGroup) {
        this.sequenceIdentifier = sequenceIdentifier;
        this.processingGroup = processingGroup;
    }

    @Override
    public Object identifier() {
        return this.sequenceIdentifier;
    }

    @Override
    public String group() {
        return this.processingGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueueIdentifier)) {
            return false;
        }
        QueueIdentifier that = (QueueIdentifier) o;
        return Objects.equals(sequenceIdentifier, that.identifier())
                && Objects.equals(processingGroup, that.group());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceIdentifier, processingGroup);
    }

    @Override
    public String toString() {
        return "EventHandlingQueueIdentifier{" +
                "sequenceIdentifier=" + sequenceIdentifier +
                ", processingGroup='" + processingGroup + '\'' +
                '}';
    }
}

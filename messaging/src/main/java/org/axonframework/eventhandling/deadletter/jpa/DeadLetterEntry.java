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

package org.axonframework.eventhandling.deadletter.jpa;

import org.axonframework.eventhandling.AbstractEventEntry;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.deadletter.Cause;
import org.axonframework.messaging.deadletter.GenericCause;
import org.axonframework.serialization.Serializer;
import org.hibernate.annotations.Formula;

import java.time.Instant;
import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

/**
 * Default DeadLetter JPA entity implementation of dead letters. Used by the {@link JpaDeadLetterQueue} to store these
 * into the database to be retried later.
 *
 * @author Mitchell Herrijgers
 */
@Entity
@Table(indexes = {
        @Index(columnList = "queueGroup,identifier"),
})
public class DeadLetterEntry extends AbstractEventEntry<byte[]> {

    @Id
    private String deadLetterId;

    @Basic(optional = false)
    private String queueGroup;

    @Basic(optional = false)
    private String identifier;

    @Basic(optional = false)
    private int index;

    @Basic(optional = false)
    private Instant deadLetteredAt;

    private Instant processingStarted;

    private String causeType;
    private String causeMessage;

    @Basic(optional = false)
    private Instant expiresAt;

    @Basic(optional = false)
    private int numberOfRetries;

    public DeadLetterEntry(String deadLetterId, String group, String identifier, int index,
                           EventMessage<?> eventMessage, Serializer serializer,
                           Instant deadLetteredAt, Instant expiresAt, Cause cause) {
        super(eventMessage, serializer, byte[].class);
        this.deadLetterId = deadLetterId;
        this.queueGroup = group;
        this.identifier = identifier;
        this.index = index;
        this.deadLetteredAt = deadLetteredAt;
        this.expiresAt = expiresAt;
        this.numberOfRetries = 0;
        if (cause != null) {
            this.causeMessage = cause.message();
            this.causeType = cause.type();
        }
    }

    /**
     * Constructor required by JPA. Do not use.
     */
    protected DeadLetterEntry() {
        // required by JPA
    }

    public String getDeadLetterId() {
        return deadLetterId;
    }


    public String getQueueGroup() {
        return queueGroup;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Integer getIndex() {
        return index;
    }

    public Instant getDeadLetteredAt() {
        return deadLetteredAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    public int getNumberOfRetries() {
        return numberOfRetries;
    }

    public void setNumberOfRetries(int numberOfRetries) {
        this.numberOfRetries = numberOfRetries;
    }

    public String getCauseType() {
        return causeType;
    }

    public String getCauseMessage() {
        return causeMessage;
    }

    public Cause getCause() {
        if (causeType == null) {
            return null;
        }
        return new GenericCause(causeType, causeMessage);
    }

    public Instant getProcessingStarted() {
        return processingStarted;
    }

    public void setProcessingStarted(Instant processingStarted) {
        this.processingStarted = processingStarted;
    }


    @Formula("concat(queueGroup,identifier)")
    private String queueIdentifierConcatenated;

    public String getQueueIdentifierConcatenated() {
        return queueIdentifierConcatenated;
    }
}

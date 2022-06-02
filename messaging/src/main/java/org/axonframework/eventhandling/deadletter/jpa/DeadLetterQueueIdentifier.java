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

import org.axonframework.messaging.deadletter.QueueIdentifier;

import java.util.Objects;
import javax.persistence.Embeddable;

@Embeddable
public class DeadLetterQueueIdentifier implements QueueIdentifier {

    private String queueGroup;
    private String identifier;

    public DeadLetterQueueIdentifier(String group, String identifier) {
        this.queueGroup = group;
        this.identifier = identifier;
    }

    protected DeadLetterQueueIdentifier() {
        // For JPA
    }

    public String getQueueGroup() {
        return queueGroup;
    }

    public void setQueueGroup(String group) {
        this.queueGroup = group;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public Object identifier() {
        return identifier;
    }

    @Override
    public String group() {
        return queueGroup;
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
        return Objects.equals(queueGroup, that.group()) && Objects.equals(identifier,
                                                                          that.identifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueGroup, identifier);
    }
}

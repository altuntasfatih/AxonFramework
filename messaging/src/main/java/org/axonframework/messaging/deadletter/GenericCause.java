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

package org.axonframework.messaging.deadletter;

import java.util.Objects;

/**
 * Generic implementation of a {@link Cause}.
 *
 * @author Steven van Beelen
 * @author Mitchel Herrijgers
 * @since 4.6.0
 */
public class GenericCause implements Cause {

    private final String type;
    private final String message;

    /**
     * Construct a cause based on the given {@code throwable}. Uses the fully qualified class name as the
     * {@link #type() type} and the {@link Throwable#getMessage()} as the {@link #message() message}.
     *
     * @param throwable The throwable to base this cause on.
     */
    public GenericCause(Throwable throwable) {
        this.type = throwable.getClass().getName();
        this.message = throwable.getMessage();
    }

    /**
     * Constructs a cause based on the give {@code type} and {@code message}.
     *
     * @param type    The type of this cause.
     * @param message The message of this cause.
     */
    public GenericCause(String type, String message) {
        this.type = type;
        this.message = message;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public String message() {
        return this.message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericCause that = (GenericCause) o;
        return Objects.equals(type, that.type) && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, message);
    }

    @Override
    public String toString() {
        return "Cause{type=[" + type + "]-message=[" + message + "]}";
    }
}

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

package org.axonframework.eventhandling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.axonframework.messaging.Message;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Token keeping track of the position before a reset was triggered. This allows for downstream components to detect
 * messages that are redelivered as part of a replay.
 *
 * @author Allard Buijze
 * @since 3.2
 */
public class ReplayToken implements TrackingToken, WrappedToken, Serializable {

    private static final long serialVersionUID = -4102464856247630944L;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    private final TrackingToken tokenAtReset;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    private final TrackingToken currentToken;
    private final transient boolean lastMessageWasReplay;

    /**
     * Creates a new TrackingToken that represents the given {@code startPosition} of a stream. It will be in replay
     * state until the position of the provided {@code tokenAtReset}. After that, the {@code tokenAtReset} will become
     * the active token and the stream will no longer be considered as replaying.
     *
     * @param tokenAtReset  The token present when the reset was triggered
     * @param startPosition The position where the token should be reset to and start replaying from
     * @return A token that represents a reset to the {@code startPosition} until the provided {@code tokenAtReset}
     */
    public static TrackingToken createReplayToken(TrackingToken tokenAtReset, TrackingToken startPosition) {
        if (tokenAtReset == null) {
            return startPosition;
        }
        if (tokenAtReset instanceof ReplayToken) {
            return createReplayToken(((ReplayToken) tokenAtReset).tokenAtReset, startPosition);
        }
        if (startPosition != null && startPosition.covers(WrappedToken.unwrapLowerBound(tokenAtReset))) {
            return startPosition;
        }
        return new ReplayToken(tokenAtReset, startPosition);
    }

    /**
     * Creates a new TrackingToken that represents the tail of the stream. It will be in replay
     * state until the position of the provided {@code tokenAtReset}. After that, the {@code tokenAtReset} will become
      * the active token and the stream will no longer be considered as replaying.
     *
     * @param tokenAtReset The token present when the reset was triggered
     * @return A token that represents a reset to the tail of the stream
     */
    public static TrackingToken createReplayToken(TrackingToken tokenAtReset) {
        return createReplayToken(tokenAtReset, null);
    }

    /**
     * Indicates whether the given message is "redelivered", as a result of a previous reset. If {@code true}, this
     * means this message has been delivered to this processor before its token was reset.
     *
     * @param message The message to inspect
     * @return {@code true} if the message is a replay
     */
    public static boolean isReplay(Message<?> message) {
        return message instanceof TrackedEventMessage
                && isReplay(((TrackedEventMessage) message).trackingToken());
    }

    /**
     * Indicates whether the given {@code trackingToken} represents a position that is part of a replay.
     *
     * @param trackingToken The token to verify
     * @return {@code true} if the token indicates a replay
     */
    public static boolean isReplay(TrackingToken trackingToken) {
        return WrappedToken.unwrap(trackingToken, ReplayToken.class)
                .map(rt -> rt.isReplay())
                .orElse(false);

    }

    /**
     * Return the relative position at which a reset was triggered for this Segment.
     * In case a replay finished or no replay is active, an {@code OptionalLong.empty()} will be returned.
     *
     * @return the relative position at which a reset was triggered for this Segment
     */
    public static OptionalLong getTokenAtReset(TrackingToken trackingToken) {
        return WrappedToken.unwrap(trackingToken, ReplayToken.class)
                           .map(rt -> rt.getTokenAtReset().position())
                .orElse(OptionalLong.empty());
    }

    /**
     * Initialize a ReplayToken, using the given {@code tokenAtReset} to represent the position at which a reset was
     * triggered. The current token is reset to the initial position.
     * <p>
     * Using the {@link #createReplayToken(TrackingToken)} is preferred, as it covers cases where a replay is started
     *
     * @param tokenAtReset The token representing the position at which the reset was triggered.
     */
    public ReplayToken(TrackingToken tokenAtReset) {
        this(tokenAtReset, null);
    }

    /**
     * Initializes a ReplayToken with {@code tokenAtReset} which represents the position at which a reset was triggered
     * and the {@code newRedeliveryToken} which represents current token.
     *
     * @param tokenAtReset       The token representing the position at which the reset was triggered
     * @param newRedeliveryToken The current token
     */
    @JsonCreator
    @ConstructorProperties({"tokenAtReset", "currentToken"})
    public ReplayToken(@JsonProperty("tokenAtReset") TrackingToken tokenAtReset,
                       @JsonProperty("currentToken") TrackingToken newRedeliveryToken) {
        this(tokenAtReset, newRedeliveryToken, true);
    }

    private ReplayToken(TrackingToken tokenAtReset,
                       TrackingToken newRedeliveryToken,
                       boolean lastMessageWasReplay) {
        this.tokenAtReset = tokenAtReset;
        this.currentToken = newRedeliveryToken;
        this.lastMessageWasReplay = lastMessageWasReplay;
    }

    /**
     * Gets the token representing the position at which the reset was triggered.
     *
     * @return the token representing the position at which the reset was triggered
     */
    public TrackingToken getTokenAtReset() {
        return tokenAtReset;
    }

    /**
     * Gets the current token.
     *
     * @return the current token
     */
    public TrackingToken getCurrentToken() {
        return currentToken;
    }

    @Override
    public TrackingToken advancedTo(TrackingToken newToken) {
        if (this.tokenAtReset == null
                || (newToken.covers(WrappedToken.unwrapUpperBound(this.tokenAtReset))
                && !tokenAtReset.covers(WrappedToken.unwrapLowerBound(newToken)))) {
            // we're done replaying
            // if the token at reset was a wrapped token itself, we'll need to use that one to maintain progress.
            if (tokenAtReset instanceof WrappedToken) {
                return ((WrappedToken) tokenAtReset).advancedTo(newToken);
            }
            return newToken;
        } else if (tokenAtReset.covers(WrappedToken.unwrapLowerBound(newToken))) {
            // we're still well behind
            return new ReplayToken(tokenAtReset, newToken, true);
        } else {
            // we're getting an event that we didn't have before, but we haven't finished replaying either
            if (tokenAtReset instanceof WrappedToken) {
                return new ReplayToken(tokenAtReset.upperBound(newToken), ((WrappedToken) tokenAtReset).advancedTo(newToken), false);
            }
            return new ReplayToken(tokenAtReset.upperBound(newToken), newToken, false);
        }
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        if (other instanceof ReplayToken) {
            return new ReplayToken(this, ((ReplayToken) other).currentToken);
        }
        return new ReplayToken(this, other);
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        return advancedTo(other);
    }

    @Override
    public boolean covers(TrackingToken other) {
        if (other instanceof ReplayToken) {
            return currentToken != null && currentToken.covers(((ReplayToken) other).currentToken);
        }
        return currentToken != null && currentToken.covers(other);
    }

    private boolean isReplay() {
        return lastMessageWasReplay;
    }

    @Override
    public TrackingToken lowerBound() {
        return WrappedToken.unwrapLowerBound(currentToken);
    }

    @Override
    public TrackingToken upperBound() {
        return WrappedToken.unwrapUpperBound(currentToken);
    }

    @Override
    public <R extends TrackingToken> Optional<R> unwrap(Class<R> tokenType) {
        if (tokenType.isInstance(this)) {
            return Optional.of(tokenType.cast(this));
        } else {
            return WrappedToken.unwrap(currentToken, tokenType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplayToken that = (ReplayToken) o;
        return Objects.equals(tokenAtReset, that.tokenAtReset) &&
                Objects.equals(currentToken, that.currentToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenAtReset, currentToken);
    }

    @Override
    public String toString() {
        return "ReplayToken{" +
                "currentToken=" + currentToken +
                ", tokenAtReset=" + tokenAtReset +
                '}';
    }

    @Override
    public OptionalLong position() {
        if(currentToken != null){
            return currentToken.position();
        }
        return OptionalLong.empty();
    }
}

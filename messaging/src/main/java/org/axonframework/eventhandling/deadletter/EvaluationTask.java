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

import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventMessageHandler;
import org.axonframework.eventhandling.ListenerInvocationErrorHandler;
import org.axonframework.messaging.deadletter.DeadLetter;
import org.axonframework.messaging.deadletter.DeadLetterEvaluationException;
import org.axonframework.messaging.deadletter.DeadLetterQueue;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Runnable} implementation used to evaluate a {@link DeadLetter} taken from the {@link DeadLetterQueue}.
 * This task is added through {@link DeadLetterQueue#onAvailable(String, Runnable)}, so we can typically assume there
 * are entries ready for evaluation.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
class EvaluationTask implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final List<EventMessageHandler> eventHandlingComponents;
    private final DeadLetterQueue<EventMessage<?>> queue;
    private final String processingGroup;
    private final TransactionManager transactionManager;
    private final ListenerInvocationErrorHandler listenerInvocationErrorHandler;

    EvaluationTask(List<EventMessageHandler> eventHandlingComponents,
                   DeadLetterQueue<EventMessage<?>> queue,
                   String processingGroup,
                   TransactionManager transactionManager,
                   ListenerInvocationErrorHandler listenerInvocationErrorHandler) {
        this.eventHandlingComponents = eventHandlingComponents;
        this.queue = queue;
        this.processingGroup = processingGroup;
        this.transactionManager = transactionManager;
        this.listenerInvocationErrorHandler = listenerInvocationErrorHandler;
    }

    @Override
    public void run() {
        try {
            evaluateDeadLetterQueue();
        } catch (Exception e) {
            logger.warn("Unexpected exception occurred during evaluation the dead letter queue", e);
        }
    }

    private void evaluateDeadLetterQueue() {
        AtomicBoolean evaluationFailed = new AtomicBoolean(false);
        Optional<DeadLetter<EventMessage<?>>> optionalLetter;
        while ((optionalLetter = takeLetter()).isPresent()) {
            DeadLetter<EventMessage<?>> letter = optionalLetter.get();
            logger.debug("Start evaluation of dead-letter [{}] for processing group [{}].",
                         letter.identifier(), processingGroup);


            UnitOfWork<? extends EventMessage<?>> unitOfWork = DefaultUnitOfWork.startAndGet(letter.message());
            unitOfWork.attachTransaction(transactionManager);
            unitOfWork.onPrepareCommit(uow -> prepareCommit(letter));
            unitOfWork.onRollback(uow -> rollback(letter, uow));
            unitOfWork.executeWithResult(() -> execute(evaluationFailed, letter));

            if (evaluationFailed.get()) {
                logger.debug("Ending the evaluation task since evaluation failed.");
                return;
            }
        }

        logger.debug("Ending the evaluation task as there are no dead-letters for queue [{}] present or left.",
                     processingGroup);
    }

    private Optional<DeadLetter<EventMessage<?>>> takeLetter() {
        return transactionManager.fetchInTransaction(() -> queue.take(processingGroup));
    }

    private Object execute(AtomicBoolean evaluationFailed, DeadLetter<EventMessage<?>> letter) throws Exception {
        for (EventMessageHandler handler : eventHandlingComponents) {
            try {
                handler.handle(letter.message());
            } catch (Exception e) {
                evaluationFailed.set(true);
                listenerInvocationErrorHandler.onError(e, letter.message(), handler);
            }
        }
        return null;
    }

    private void rollback(DeadLetter<EventMessage<?>> letter, UnitOfWork<? extends EventMessage<?>> uow) {
        try {
            letter.requeue();
            logger.warn(
                    "Reentered dead-letter [{}] for processing group [{}] in the queue since evaluation failed.",
                    letter.identifier(),
                    processingGroup,
                    uow.getExecutionResult().getExceptionResult()
            );
        } catch (Exception e) {
            throw new DeadLetterEvaluationException(
                    "Failed while enqueueing dead-letter [" + letter.identifier()
                            + "] again after a failed evaluation.", e
            );
        }
    }

    private void prepareCommit(DeadLetter<EventMessage<?>> letter) {
        try {
            letter.acknowledge();
            logger.info(
                    "Dead-letter [{}] is acknowledged as it is successfully handled for processing group [{}].",
                    letter.identifier(), processingGroup
            );
        } catch (Exception e) {
            throw new DeadLetterEvaluationException(
                    "Failed while acknowledging dead-letter [" + letter.identifier()
                            + "] after successfully evaluation.", e
            );
        }
    }
}

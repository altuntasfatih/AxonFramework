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

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.common.transaction.Transaction;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.deadletter.DeadLetteringEventHandlerInvoker;
import org.axonframework.eventhandling.deadletter.DeadLetteringEventIntegrationTest;
import org.axonframework.messaging.deadletter.DeadLetterQueue;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executors;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import static org.mockito.Mockito.*;

/**
 * An implementation of the {@link DeadLetteringEventIntegrationTest} validating the {@link JpaDeadLetterQueue} with an
 * {@link org.axonframework.eventhandling.EventProcessor} and {@link DeadLetteringEventHandlerInvoker}.
 *
 * @author Mitchell Herrijgers
 */
class JpaDeadLetteringIntegrationTest extends DeadLetteringEventIntegrationTest {

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("dlq");
    EntityManager entityManager = emf.createEntityManager();
    private final TransactionManager transactionManager = spy(new NoOpTransactionManager());
    private EntityTransaction transaction;

    @BeforeEach
    public void setUpJpa() throws SQLException {
        transaction = entityManager.getTransaction();
        transaction.begin();
    }

    @AfterEach
    public void rollback() {
        entityManager.createQuery("DELETE FROM DeadLetterEntry dl").executeUpdate();
        entityManager.flush();
        entityManager.clear();
        transaction.commit();
    }

    @Override
    protected DeadLetterQueue<EventMessage<?>> buildDeadLetterQueue() {
        EntityManagerProvider entityManagerProvider = new SimpleEntityManagerProvider(entityManager);
        return JpaDeadLetterQueue.<EventMessage<?>>builder()
                                 .transactionManager(transactionManager)
                                 .entityManagerProvider(entityManagerProvider)
                                 .expireThreshold(Duration.ofMillis(50))
                                 .scheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                                 .build();
    }

    /**
     * A non-final {@link TransactionManager} implementation, so that it can be spied upon through Mockito.
     */
    private static class NoOpTransactionManager implements TransactionManager {

        @Override
        public Transaction startTransaction() {
            return new Transaction() {
                @Override
                public void commit() {
                    // No-op
                }

                @Override
                public void rollback() {
                    // No-op
                }
            };
        }
    }
}

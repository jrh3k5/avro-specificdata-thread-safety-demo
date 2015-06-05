package com.github.jrh3k5;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadFactory;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBuilderBase;
import org.junit.Test;

import com.github.jrh3k5.avro.TestObject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tests demonstrating thread safety issues with {@link SpecificData}.
 * <p />
 * Specifically, implementations of {@link SpecificRecordBuilderBase} use {@link SpecificData#get()}, which references a static instance of the variable used across all threads. When this is used to
 * build the default value of {@code null} for a {@code byte} array, there seems to be intermitten issues - {@link BufferUnderflowException} or {@link ArrayIndexOutOfBoundsException}.
 * 
 * @author Joshua Hyde
 */

public class SpecificDataThreadSafetyTest {
    /**
     * Execute the test.
     * 
     * @throws Exception
     *             If any errors occur during the test run.
     */
    @Test
    public void testThreadSafety() throws Exception {
        final CataloguingExceptionHandler exceptionHandler = new CataloguingExceptionHandler();
        final ThreadFactoryBuilder factoryBuilder = new ThreadFactoryBuilder().setNameFormat("thread-safety-demo-%d").setUncaughtExceptionHandler(exceptionHandler);
        final ThreadFactory threadFactory = factoryBuilder.build();

        final List<Thread> threads = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            threads.add(threadFactory.newThread(new ThreadSafetyRunnable()));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        if (exceptionHandler.getErrors().isEmpty()) {
            System.out.println("Much to our surprise, there are no errors!");
        } else {
            System.out.println(String.format("The following %d threads had failures:", exceptionHandler.getErrors().size()));
            for (Entry<String, List<Throwable>> error : exceptionHandler.getErrors().entrySet()) {
                System.out.println(String.format("Thread %s had the following errors:", error.getKey()));
                for(Throwable throwable : error.getValue()) {
                    throwable.printStackTrace(System.err);
                }
            }
            
            throw new AssertionError(String.format("%d threads failed; see log for details.", exceptionHandler.getErrors().size()));
        }
    }

    private static class ThreadSafetyRunnable implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < 5000; i++) {
                TestObject.newBuilder().build();
            }
        }
    }

    private static class CataloguingExceptionHandler implements UncaughtExceptionHandler {
        private final Map<String, List<Throwable>> errors = Collections.synchronizedMap(new HashMap<String, List<Throwable>>());

        public Map<String, List<Throwable>> getErrors() {
            return Collections.unmodifiableMap(errors);
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (!errors.containsKey(t.getName())) {
                errors.put(t.getName(), new ArrayList<Throwable>());
            }
            errors.get(t.getName()).add(e);
        }
    }
}

package net.ckozak;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.threads.EnhancedQueueExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@org.openjdk.jmh.annotations.State(Scope.Benchmark)
@Measurement(iterations = 4, time = 7)
@Warmup(iterations = 2, time = 7)
// Use a consistent heap and GC configuration to avoid confusion between JVMS with differing GC defaults.
@Fork(
        value = 1,
        jvmArgs = {"-Xmx2g", "-Xms2g", "-XX:+UseParallelOldGC", "-XX:-UseBiasedLocking", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints"})
public class ExecutorBenchmarks {

    // this can be used to avoid the near-empty syndrome
    static final long DELAY_CONSUMER = Long.getLong("delay.c", 0L);

    @Param({"LTQ_THREAD_POOL_EXECUTOR", "LBQ_THREAD_POOL_EXECUTOR", "EQE_NO_LOCKS", "EQE_SPIN_LOCK"})
    public ExecutorType factory;

    @Param ({ "1", "2", "4", "8", "14", "28" })
    public int cores;

    @Param ({ "16,16", "100,200" })
    public String executorThreads;

    @Param( {"1", "10", "100"})
    public int burstSize;

    @Param( {"false", "true"})
    public boolean spinWaitCompletion;

    public enum ExecutorType {
        LBQ_THREAD_POOL_EXECUTOR(LockMode.NO_LOCKS /* any value */),
        LTQ_THREAD_POOL_EXECUTOR(LockMode.NO_LOCKS /* any value */),
        EQE_NO_LOCKS(LockMode.NO_LOCKS),
        EQE_SPIN_LOCK(LockMode.SPIN_LOCK);

        private final LockMode mode;

        ExecutorType(LockMode mode) {
            this.mode = mode;
        }

        ExecutorService create(String executorThreads) {
            mode.install();
            String[] coreAndMax = executorThreads.split(",");
            if (coreAndMax.length != 2) {
                throw new IllegalStateException("Failed to parse " + executorThreads);
            }
            int coreThreads = Integer.parseInt(coreAndMax[0].trim());
            int maxThreads = Integer.parseInt(coreAndMax[1].trim());
            ExecutorService executorService = createExecutor(
                    coreThreads,
                    maxThreads,
                    Duration.ofMinutes(1),
                    Executors.defaultThreadFactory());
            startCoreThreads(executorService, coreThreads);
            return executorService;
        }

        ExecutorService createExecutor(
                int coreThreads,
                int maxThreads,
                Duration keepAliveTime,
                ThreadFactory threadFactory) {
            if (this == LBQ_THREAD_POOL_EXECUTOR) {
                return new ThreadPoolExecutor(coreThreads, maxThreads,
                        keepAliveTime.toNanos(), TimeUnit.NANOSECONDS,
                        new LinkedBlockingQueue<>(),
                        threadFactory);
            } else if (this == LTQ_THREAD_POOL_EXECUTOR) {
                return new ThreadPoolExecutor(coreThreads, maxThreads,
                        keepAliveTime.toNanos(), TimeUnit.NANOSECONDS,
                        new LinkedTransferQueue<>(),
                        threadFactory);
            }
            return new EnhancedQueueExecutor.Builder()
                    .setCorePoolSize(coreThreads)
                    .setMaximumPoolSize(maxThreads)
                    .setKeepAliveTime(keepAliveTime)
                    .setThreadFactory(threadFactory)
                    .build();
        }

        static void startCoreThreads(ExecutorService executorService, int coreThreads) {
            // Ensure all threads have started, and aren't queued for an unexpectedly smaller pool
            CountDownLatch threadsStartedLatch = new CountDownLatch(coreThreads);
            CountDownLatch completionLatch = new CountDownLatch(1);
            Runnable runnable = () -> {
                try {
                    threadsStartedLatch.countDown();
                    completionLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            for (int i = 0; i < coreThreads; i++) {
                executorService.execute(runnable);
            }
            try {
                threadsStartedLatch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            completionLatch.countDown();
        }
    }

    public enum LockMode {
        NO_LOCKS,
        SPIN_LOCK;

        void install() {
            switch (this) {
                case NO_LOCKS:
                    System.setProperty("jboss.threads.eqe.tail-lock", "false");
                    System.setProperty("jboss.threads.eqe.head-lock", "false");
                    break;
                case SPIN_LOCK:
                    // default
                    break;
                default:
                    throw new IllegalStateException("Unknown state: " + this);
            }
        }
    }
    private ExecutorService executor;

    @Setup
    public void setup() {
        CoreAffinity.useCores(cores);
        executor = factory.create(executorThreads);
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        executor.shutdownNow();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("executor failed to teminate within 1 second");
        }
    }

    @State(Scope.Thread)
    public static class ExecutingTasks implements Runnable {

        Future<?>[] pendingTasks;
        int burstSize;
        boolean spinWaitCompletion;

        @Setup(Level.Iteration)
        public void init(ExecutorBenchmarks benchmarks) {
            burstSize = benchmarks.burstSize;
            pendingTasks = new FutureTask<?>[benchmarks.burstSize];
            spinWaitCompletion = benchmarks.spinWaitCompletion;
        }

        @Override
        public void run()
        {
            if (DELAY_CONSUMER > 0) {
                Blackhole.consumeCPU(DELAY_CONSUMER);
            }
        }

        public void submitBurst(ExecutorService executor)
        {
            Future<?>[] pendingTasks = this.pendingTasks;
            for (int i = 0, size = burstSize; i < size; i++)
            {
                pendingTasks[i] = executor.submit(this);
            }
        }

        public void awaitBurstCompletion() throws ExecutionException, InterruptedException
        {
            // with spinWaitCompletion it attempt to check completion of previous tasks
            // while awaiting the current task to complete
            boolean spinWaitCompletion = this.spinWaitCompletion;
            Future<?>[] pendingTasks = this.pendingTasks;
            int lowerIndexToCheck = 0;
            for (int i = 0, size = burstSize; i < size; i++) {
                int index = size - (i+1);
                if (index < lowerIndexToCheck)
                {
                    break;
                }
                Future<?> lastTask = pendingTasks[index];
                if (lastTask != null)
                {
                    pendingTasks[index] = null;
                    if (!spinWaitCompletion)
                    {
                        lastTask.get();
                    }
                    else
                    {
                        while (!lastTask.isDone())
                        {
                            final int remaining = index - lowerIndexToCheck;
                            if (remaining > 0)
                            {
                                lowerIndexToCheck = deleteCompleted(pendingTasks, lowerIndexToCheck, remaining);
                            } else {
                                /* Should happen only when this task is the very last one */
                                Thread.onSpinWait();
                            }
                        }
                    }
                }
            }
        }

        private static int deleteCompleted(Future<?>[] pendingTasks, int start, int length)
        {
            int lowWaterMark = start;
            for (int i = start; i < length; i++)
            {
                Future<?> lastTask = pendingTasks[i];
                // you can still get holes in the middle
                if (lastTask != null)
                {
                    if (lastTask.isDone())
                    {
                        pendingTasks[i] = null;
                        if (lowWaterMark == i)
                        {
                            lowWaterMark++;
                        }
                    }
                }
            }
            return lowWaterMark;
        }
    }

    @Threads(32)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void benchmarkSubmit(ExecutingTasks executingTasks) throws Exception {
        executingTasks.submitBurst(executor);
        executingTasks.awaitBurstCompletion();
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder()
                .include(ExecutorBenchmarks.class.getSimpleName())
                .build())
                .run();
    }
}

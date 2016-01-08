package com.airbnb.di.multiprocessing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Executes a job as a thread
 *
 * @param <T>
 */
public class Worker <T extends Job> extends Thread {

    private static final Log LOG = LogFactory.getLog(Worker.class);

    private static int nextWorkerId = 0;

    private int workerId;
    private BlockingQueue<T> inputQueue;
    private ParallelJobExecutor parallelJobExecutor;
    private Job job = null;

    public Worker(BlockingQueue<T> inputQueue, ParallelJobExecutor parallelJobExecutor) {
        this.inputQueue = inputQueue;
        this.workerId = nextWorkerId++;
        this.parallelJobExecutor = parallelJobExecutor;
        setName(Worker.class.getSimpleName() + "-" + workerId);
        setDaemon(true);
    }

    public Worker(String workerNamePrefix,
                  BlockingQueue<T> inputQueue,
                  ParallelJobExecutor parallelJobExecutor) {
        this.inputQueue = inputQueue;
        this.workerId = nextWorkerId++;
        this.parallelJobExecutor = parallelJobExecutor;
        setName(workerNamePrefix + "-" + workerId);
        setDaemon(true);
    }

    @Override
    public void run()  {
        try {
            while (true) {
                if (job == null) {
                    LOG.debug("Waiting for a job");
                    job = inputQueue.take();
                } else {
                    LOG.debug("Using existing job");
                }
                LOG.debug("**** Running job: " + job + " ****");
                int ret = job.run();
                if (ret != 0) {
                    LOG.error("Error running job " + job + " return code: " +
                            ret);
                    // TODO: Need to recover from failures with retires
                    System.exit(-1);
                }
                LOG.debug("**** Done running job: " + job + " ****");
                parallelJobExecutor.notifyDone(job);
                job = null;
            }
        } catch (InterruptedException e) {
            LOG.debug("Got interrupted");
        } catch (RuntimeException e) {
            LOG.error("Worker got a runtime exception: ", e);
            System.exit(-1);
        }
    }

    public Job getJob() {
        return job;
    }
}

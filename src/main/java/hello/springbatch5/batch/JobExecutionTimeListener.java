package hello.springbatch5.batch;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
public class JobExecutionTimeListener extends JobExecutionListenerSupport {

    private long startTime;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = System.currentTimeMillis();
        System.out.println("Job started: " + jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Job finished: " + jobExecution.getJobInstance().getJobName());
        System.out.println("Execution time: " + duration + " ms");
    }
}

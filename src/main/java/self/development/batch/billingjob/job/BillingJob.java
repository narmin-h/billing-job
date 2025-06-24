package self.development.batch.billingjob.job;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.repository.JobRepository;


public class BillingJob implements Job {

    private JobRepository jobRepository;

    public BillingJob(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Override
    public String getName() {
        return "Billing Job";
    }

    @Override
    public void execute(JobExecution execution) {
        var parameters = execution.getJobParameters();
        var result = parameters.getString("input.file");
        System.out.println("Getting job information "+result);
        execution.setStatus(BatchStatus.COMPLETED);
        execution.setExitStatus(ExitStatus.COMPLETED);
        this.jobRepository.update(execution);
    }


//    @Override
//    public void execute(JobExecution execution) {
//        try {
//            System.out.println("Throwing exception");
//            throw new Exception("Throwing exception");
//        } catch (Exception ex) {
//            execution.addFailureException(ex);
//            execution.setStatus(BatchStatus.COMPLETED);
//            execution.setExitStatus(ExitStatus.FAILED.addExitDescription(ex.getMessage()));
//        } finally {
//            this.jobRepository.update(execution);
//        }
//
//    }
//


}

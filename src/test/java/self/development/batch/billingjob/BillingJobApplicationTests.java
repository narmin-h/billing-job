package self.development.batch.billingjob;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

@SpringBootTest
@SpringBatchTest
@ExtendWith(OutputCaptureExtension.class)
class BillingJobApplicationTests {

    @Autowired
    private Job job;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;


    @BeforeEach
    public void setUp() {
        this.jobRepositoryTestUtils.removeJobExecutions();
        //jobLauncherTestUtils.setJob(job);
        JdbcTestUtils.deleteFromTables(jdbcTemplate,"BILLING_DATA");
    }

    @Test
    void testOutput(CapturedOutput capturedOutput)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException {

        // Given
        JobParameters jobParameter = new JobParameters();

        // When
        var result = jobLauncher.run(job, jobParameter);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        //Assertions.assertTrue(capturedOutput.getOut().contains("Throwing exception"));
    }

    @Test
    void testOutputWithParams(CapturedOutput capturedOutput)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException, InterruptedException {

        // Given
        var parameters = new JobParametersBuilder()
                .addString("input.file","src/main/resources/input11.csv")
                .toJobParameters();

        // When
        var result = jobLauncher.run(job, parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        Assertions.assertTrue(capturedOutput.getOut().contains("Getting job information src/main/resources/input11.csv"));
    }

    @Test
    void testOutputWithTestUtils(CapturedOutput capturedOutput)
            throws Exception {

        // Given
        var parameters = new JobParametersBuilder()
                .addString("input.file","src/main/resources/input11.csv")
                .toJobParameters();

        // When
        var result = jobLauncherTestUtils.launchJob(parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        Assertions.assertTrue(capturedOutput.getOut().contains("Getting job information src/main/resources/input11.csv"));
    }

    @Test
    void testOutputWithTestUtilsUniqueParams(CapturedOutput capturedOutput)
            throws Exception {

        // Given
        var parameters = this.jobLauncherTestUtils.getUniqueJobParametersBuilder()
                .addString("input.file","src/main/resources/input11.csv")
                .toJobParameters();

        // When
        var result = jobLauncherTestUtils.launchJob(parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        Assertions.assertTrue(capturedOutput.getOut().contains("Getting job information src/main/resources/input11.csv"));
    }

    @Test
    void testOutputWithTestUtilsPath(CapturedOutput capturedOutput)
            throws Exception {
        // Given
        var parameters = new JobParametersBuilder()
                .addString("input.file","src/main/resources/input2.csv")
                .toJobParameters();

        // When
        var result = jobLauncherTestUtils.launchJob(parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        Assertions.assertTrue(Files.exists(Paths.get("staging", "input2.csv")));
    }

    @Test
    void testOutputWithTestUtilsPathWithException(CapturedOutput capturedOutput)
            throws Exception {

        // Given
        // No parameters needed for this test

        // When
        var result = jobLauncherTestUtils.launchJob();

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
    }

    @Test
    void testOutputWithItemReaderWriter(CapturedOutput capturedOutput)
            throws Exception {
        // Given
        var parameters = new JobParametersBuilder()
                .addString("input.file","src/main/resources/telecom_data.csv")
                .toJobParameters();

        // When
        var result = jobLauncherTestUtils.launchJob(parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        Assertions.assertTrue(Files.exists(Paths.get("staging", "telecom_data.csv")));
        Assertions.assertEquals(260, JdbcTestUtils.countRowsInTable(jdbcTemplate, "BILLING_DATA"));
    }


    @Test
    void testOutputWithProcessorStep(CapturedOutput capturedOutput)
            throws Exception {
        // Given
        var parameters = new JobParametersBuilder()
                .addString("input.file","src/main/resources/telecom_data.csv")
                .toJobParameters();

        // When
        var result = jobLauncherTestUtils.launchJob(parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
        Assertions.assertTrue(Files.exists(Paths.get("staging", "telecom_data.csv")));
        Assertions.assertEquals(260, JdbcTestUtils.countRowsInTable(jdbcTemplate, "BILLING_DATA"));
        var billingReport = Paths.get("staging", "processed-report.csv");
        Assertions.assertTrue(Files.exists(billingReport));
        Assertions.assertEquals(252, Files.lines(billingReport).count());
    }


    @Test
    void testOutputWithSpel(CapturedOutput capturedOutput)
            throws Exception {
        // Given
        var parameters = new JobParametersBuilder()
                .addString("input.file","src/main/resources/telecom_data_next.csv")
                .addString("output.file", "staging/processed-result-test.csv")
                .addJobParameter("data.year", 2025, Integer.class)
                .addJobParameter("data.month", 3, Integer.class)
                .toJobParameters();

        // When
        var result = jobLauncherTestUtils.launchJob(parameters);

        // Then
        Assertions.assertEquals(result.getStatus(), BatchStatus.COMPLETED);
    }

}

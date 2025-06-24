package self.development.batch.billingjob.config;

import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.support.JdbcTransactionManager;
import self.development.batch.billingjob.exception.PricingException;
import self.development.batch.billingjob.job.BillingJob;
import self.development.batch.billingjob.model.BillingData;
import self.development.batch.billingjob.model.ReportingData;
import self.development.batch.billingjob.processor.BillingDataProcessor;
import self.development.batch.billingjob.service.PricingService;
import self.development.batch.billingjob.skip.BillingDataSkipListener;
import self.development.batch.billingjob.task.FilePreparationTasklet;

@Configuration
public class BillingConfig {


    @Bean("billingJob")
    public Job job(JobRepository jobRepository) {
        return new BillingJob(jobRepository);
    }

    @Bean("mainJob")
    @Primary
    public Job mainJob(JobRepository jobRepository, Step copyStep,
                       Step fileIngestionStep,
                       Step processorStep,
                       JobParametersValidator defaultJobParametersValidator) {
        return new JobBuilder("mainJob", jobRepository)
                .start(copyStep)
                .next(fileIngestionStep)
                .next(processorStep)
                .validator(defaultJobParametersValidator)
                //.validator(new DefaultJobParametersValidator(new String[] {"input.file"}, new String[] {}))
                .build();
    }

    @Bean
    public Step copyStep(JobRepository jobRepository, JdbcTransactionManager manager) {
        return new StepBuilder("copyStep", jobRepository)
                .tasklet(new FilePreparationTasklet(), manager)
                .build();
    }

    @Bean
    public Step fileIngestionStep(JobRepository jobRepository, JdbcTransactionManager manager,
                                  FlatFileItemReader<BillingData> billingDataFlatFileItemReader,
                                  JdbcBatchItemWriter<BillingData> billingDataJdbcBatchItemWriter,
                                  BillingDataSkipListener skipListener) {

        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(100, manager)
                .reader(billingDataFlatFileItemReader)
                .writer(billingDataJdbcBatchItemWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(10)
                .listener(skipListener)
                .build();
    }

    @Bean
    public Step processorStep(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                      ItemReader<BillingData> billingDataTableReader,
                      ItemProcessor<BillingData, ReportingData> billingDataProcessor,
                      ItemWriter<ReportingData> billingDataFileWriter) {
        return new StepBuilder("reportGeneration", jobRepository)
                .<BillingData, ReportingData>chunk(100, transactionManager)
                .reader(billingDataTableReader)
                .processor(billingDataProcessor)
                .writer(billingDataFileWriter)
                .faultTolerant()
                .retry(PricingException.class)
                .retryLimit(100)
                .build();
    }

    @Bean
    public JobParametersValidator defaultJobParametersValidator() {
        return new DefaultJobParametersValidator(
                new String[]{"input.file"}, // Required parameters
                new String[]{} // Optional parameters
        );
    }

    @Bean
    @StepScope
    public FlatFileItemReader<BillingData> billingDataFlatFileItemReader(@Value("#{jobParameters['input.file']}") String inputFile) {
        return new FlatFileItemReaderBuilder<BillingData>()
                .name("billingDataFlatFileItemReader")
                .resource(new FileSystemResource(inputFile))
                .delimited()
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage","callDuration", "smsCount")
                .linesToSkip(1)
                .targetType(BillingData.class)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<BillingData> billingDataJdbcBatchItemWriter(DataSource dataSource) {
        var statement = "insert into BILLING_DATA values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)";
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(statement)
                .beanMapped()
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<BillingData> billingDataTableReader(DataSource dataSource,
                                                                    @Value("#{jobParameters['data.year']}") Integer year,
                                                                    @Value("#{jobParameters['data.month']}") Integer month) {
        String sql = String.format("select * from BILLING_DATA where DATA_YEAR = %d and DATA_MONTH = %d", year, month);
        return new JdbcCursorItemReaderBuilder<BillingData>()
                .name("billingDataTableReader")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                .build();
    }

    @Bean
    public BillingDataProcessor billingDataProcessor(PricingService pricingService) {
        return new BillingDataProcessor(pricingService);
    }

//    @Bean
//    public PricingService pricingService() {
//        return new PricingService();
//    }

    @Bean
    @StepScope
    public FlatFileItemWriter<ReportingData> billingDataFileWriter(@Value("#{jobParameters['output.file']}") String outputFile) {
        return new FlatFileItemWriterBuilder<ReportingData>()
                .resource(new FileSystemResource(outputFile))
                .name("billingDataFileWriter")
                .delimited()
                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
                .build();
    }

    @Bean
    @StepScope
    public BillingDataSkipListener skipListener(@Value("#{jobParameters['skip.file']}") String skippedFile) {
        return new BillingDataSkipListener(skippedFile);
    }
}

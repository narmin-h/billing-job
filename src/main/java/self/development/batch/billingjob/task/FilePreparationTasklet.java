package self.development.batch.billingjob.task;

import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class FilePreparationTasklet implements Tasklet {

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var parameters = contribution.getStepExecution().getJobParameters();
        var parameter = parameters.getString("input.file");
        var path = Paths.get(parameter);
        var to = Paths.get("staging", path.toFile().getName());
        Files.copy(path, to, StandardCopyOption.REPLACE_EXISTING);
        return RepeatStatus.FINISHED;
    }
}

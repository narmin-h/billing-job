package self.development.batch.billingjob.skip;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import self.development.batch.billingjob.model.BillingData;

public class BillingDataSkipListener implements SkipListener<BillingData, BillingData> {

    Path skippedItemsFile;

    public BillingDataSkipListener(String skippedItemsFile) {
        this.skippedItemsFile = Paths.get(skippedItemsFile);
    }

    @Override
    public void onSkipInRead(Throwable throwable) {
        if (throwable instanceof FlatFileParseException exception) {
            String rawLine = exception.getInput();
            int lineNumber = exception.getLineNumber();
            String skippedLine = lineNumber + "|" + rawLine + System.lineSeparator();
            try {
                Files.writeString(this.skippedItemsFile, skippedLine, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw new RuntimeException("Unable to write skipped item " + skippedLine);
            }
        }
    }
}
package self.development.batch.billingjob.exception;

public class PricingException extends RuntimeException {

    public PricingException(String errorWhileRetrievingDataPricing) {
        super(errorWhileRetrievingDataPricing);
    }
}

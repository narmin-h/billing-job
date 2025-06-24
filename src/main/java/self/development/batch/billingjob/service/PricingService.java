package self.development.batch.billingjob.service;

import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import self.development.batch.billingjob.exception.PricingException;

@Slf4j
@Service
public class PricingService {


    private float dataPricing = 0.01f ;
    private float callPricing = 0.5f;
    private float smsPricing = 0.1f;

    private Random random = new Random();

    public float getDataPricing() {
        simulateException();
        return this.dataPricing;
    }

    public float getCallPricing() {
        simulateException();
        return this.callPricing;
    }

    public float getSmsPricing() {
        simulateException();
        return this.smsPricing;
    }

    private void simulateException() {
        if (this.random.nextInt(1000) % 7 == 0) {
            log.info("Throwing exception...");
            throw new PricingException("Error while retrieving data pricing");
        }
    }
}
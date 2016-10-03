package pe.joedayz.etl.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import pe.joedayz.etl.ws.BolSpring;



@Service
@Transactional(propagation = Propagation.NOT_SUPPORTED)
public class GeneradorJobImpl {
	
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private BolSpring bolSpring;
    
    
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    @Scheduled(initialDelay = 30000, fixedRate = 10000)  // initial delay of 30segs, run every 10 seconds
    public void scheduleGeneratorJobs() {
    	
    	log.info("Extraer boleta");
    	bolSpring.extraerBoleta();
    	
    }
}

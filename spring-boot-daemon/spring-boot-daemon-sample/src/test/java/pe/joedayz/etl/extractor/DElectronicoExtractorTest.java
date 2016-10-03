package pe.joedayz.etl.extractor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import pe.joedayz.etl.SampleApplication;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SampleApplication.class)
public class DElectronicoExtractorTest {

	
	private static Logger log = LoggerFactory.getLogger(DElectronicoExtractorTest.class);
	
	@Autowired
	private DElectronicoExtractor extractor;
	
	@Test
	public void deberiaObtenerPendienteDocElectronico(){
		extractor.pendienteDocElectronico();
		
	}
}

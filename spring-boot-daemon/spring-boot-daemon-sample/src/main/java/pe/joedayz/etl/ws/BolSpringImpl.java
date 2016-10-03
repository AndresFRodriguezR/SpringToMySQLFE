package pe.joedayz.etl.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.joedayz.etl.beans.DocumentoBean;
import pe.joedayz.etl.extractor.DElectronicoExtractor;

@Service
public class BolSpringImpl implements BolSpring {

	private final Logger log = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private DElectronicoExtractor extractor;
	
	@Override
	public String extraerBoleta() {
		
		DocumentoBean nuevo = extractor.pendienteDocElectronico();
		

		
		return null;
	}

}

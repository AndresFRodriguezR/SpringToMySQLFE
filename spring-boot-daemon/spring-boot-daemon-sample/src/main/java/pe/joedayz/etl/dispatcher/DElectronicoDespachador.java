package pe.joedayz.etl.dispatcher;

import java.sql.SQLException;
import java.util.List;

import pe.joedayz.etl.beans.DocumentoBean;

public interface DElectronicoDespachador {

	DocumentoBean cargarDocElectronico(String pdocu_codigo);
	
	List<DocumentoBean> cargarDetDocElectronico(String pdocu_codigo) throws SQLException;
	
	boolean yaExisteDocElectronico(DocumentoBean pItems);
	
	DocumentoBean noPendienteDocElectronico();
	
	DocumentoBean insertarDocElectronico(DocumentoBean pItems, List<DocumentoBean> plItems); 
}

package pe.joedayz.etl.dispatcher;

import java.util.List;

import pe.joedayz.etl.beans.DocumentoBean;

public interface ResumenBajaDespachador {

	
	
	DocumentoBean cargarCabecera(String trans);
	List<DocumentoBean> cargarResumen(String trans) throws Exception;
}

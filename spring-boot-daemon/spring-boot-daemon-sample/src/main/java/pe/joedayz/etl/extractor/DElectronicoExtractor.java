package pe.joedayz.etl.extractor;

import java.sql.SQLException;
import java.util.List;

import pe.joedayz.etl.beans.DocumentoBean;
import pe.joedayz.etl.beans.OtrosDetalles;

public interface DElectronicoExtractor {

	DocumentoBean cargarDEFactura(DocumentoBean pItems);
	
	DocumentoBean cargarDEBoleta(DocumentoBean pItems);
	
	DocumentoBean cargarDENCredito(DocumentoBean pItems);
	
	DocumentoBean cargarDENDebito(DocumentoBean pItems);
	
	List<DocumentoBean> cargarDetDocElectronico(DocumentoBean pItem) throws SQLException;
	
	List<OtrosDetalles> cargarDetDocElectronicoDet(DocumentoBean pItem, String codProducto) throws SQLException;
	
	DocumentoBean pendienteDocElectronico();
	
	DocumentoBean noPendienteDocElectronico(); 
	
}

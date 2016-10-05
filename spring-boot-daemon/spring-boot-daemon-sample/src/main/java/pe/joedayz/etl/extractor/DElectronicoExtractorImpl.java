package pe.joedayz.etl.extractor;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;



import pe.joedayz.etl.beans.DocumentoBean;
import pe.joedayz.etl.beans.OtrosDetalles;
import pe.joedayz.etl.support.WhereParams;

@Repository
public class DElectronicoExtractorImpl implements DElectronicoExtractor {

	private static final Logger LOGGER = LoggerFactory.getLogger(DElectronicoExtractorImpl.class);
	 
	@Autowired
	DataSource dataSource;

	private NamedParameterJdbcTemplate jdbcTemplate;

	@PostConstruct
	public void init() {
		jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}

	@Override
	public DocumentoBean cargarDEFactura(DocumentoBean pItems) {

		return null;
	}
	
	

	@Override
	public DocumentoBean cargarDEBoleta(DocumentoBean pItems) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentoBean cargarDENCredito(DocumentoBean pItems) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentoBean cargarDENDebito(DocumentoBean pItems) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DocumentoBean> cargarDetDocElectronico(DocumentoBean pItem) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<OtrosDetalles> cargarDetDocElectronicoDet(DocumentoBean pItem, String codProducto) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	private static final String SEARCH_DOCUMENTS = "SELECT TOP 1 companiasocio as clie_numero, tipodocumento as docu_tipodocumento, numerodocumento as docu_numero \n" +
				" FROM efactura \n" + 
				" WHERE  nuevo = 'N' \n" +
				" order by FechaDocumento ";

	@Override
	public DocumentoBean pendienteDocElectronico() {

		Map<String, String> queryParams = new HashMap<>();
        
        List<DocumentoBean> searchResults = jdbcTemplate.query(SEARCH_DOCUMENTS,
                queryParams,
                new BeanPropertyRowMapper<>(DocumentoBean.class)
        );
        
        LOGGER.info("Found documents {}", searchResults);
        
        if (searchResults.size() > 0) {
      	
        	updateStatus(searchResults.get(0));
            return searchResults.get(0);
        }        
		return null;
	}

	private void updateStatus(DocumentoBean documentoBean) {
        WhereParams params = new WhereParams();
        StringBuilder sql = new StringBuilder();

        sql.append("update ");
        sql.append(params.filter("efactura set nuevo='P' where companiasocio= :clie_numero ", documentoBean.getClie_numero()));
        sql.append(params.filter(" and tipodocumento = :docu_tipodocumento ", documentoBean.getDocu_tipodocumento()));
        sql.append(params.filter(" and numerodocumento = :docu_numero ", documentoBean.getDocu_numero()));
        int rowsUpdate = jdbcTemplate.update(sql.toString(), params.getParams());
        LOGGER.info("Documento modificado " + documentoBean + ", rows " + rowsUpdate);
	}

	@Override
	public DocumentoBean noPendienteDocElectronico() {
		// TODO Auto-generated method stub
		return null;
	}

}

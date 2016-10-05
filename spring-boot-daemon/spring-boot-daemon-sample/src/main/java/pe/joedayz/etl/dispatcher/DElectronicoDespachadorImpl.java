package pe.joedayz.etl.dispatcher;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import pe.joedayz.etl.beans.DocumentoBean;
import pe.joedayz.etl.extractor.DElectronicoExtractorImpl;
import pe.joedayz.etl.support.WhereParams;


@Repository
public class DElectronicoDespachadorImpl implements DElectronicoDespachador {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(DElectronicoDespachadorImpl.class);

	@Autowired
	DataSource dataSource;

	private NamedParameterJdbcTemplate jdbcTemplate;

	@PostConstruct
	public void init() {
		jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	
	@Override
	public DocumentoBean cargarDocElectronico(String pdocu_codigo) {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT DOCU_CODIGO,");
        sql.append( " EMPR_RAZONSOCIAL,");
        sql.append( " EMPR_UBIGEO,");
        sql.append( " EMPR_NOMBRECOMERCIAL,");
        sql.append( " EMPR_DIRECCION,");
        sql.append( " EMPR_PROVINCIA,");
        sql.append( " EMPR_DEPARTAMENTO,");
        sql.append( " EMPR_DISTRITO,");
        sql.append( " EMPR_PAIS,");
        sql.append( " EMPR_NRORUC,");
        sql.append( " EMPR_TIPODOC,");
        sql.append( " CLIE_NUMERO,");
        sql.append( " CLIE_TIPODOC,");
        sql.append( " CLIE_NOMBRE,");
        sql.append( " DOCU_FECHA,");
        sql.append( " DOCU_TIPODOCUMENTO,");
        sql.append( " DOCU_NUMERO,");
        sql.append( " DOCU_MONEDA,");
        sql.append( " DOCU_GRAVADA  as  DOCU_GRAVADA,");
        sql.append( " DOCU_INAFECTA  as  DOCU_INAFECTA,");
        sql.append( " DOCU_EXONERADA  as  DOCU_EXONERADA,");
        sql.append( " DOCU_GRATUITA  as  DOCU_GRATUITA,");
        sql.append( " DOCU_DESCUENTO  as  DOCU_DESCUENTO,");
        sql.append( " DOCU_SUBTOTAL  as  DOCU_SUBTOTAL,");
        sql.append( " DOCU_TOTAL  as  DOCU_TOTAL,");
        sql.append( " DOCU_IGV  as  DOCU_IGV,");
        sql.append( " TASA_IGV,");
        sql.append( " DOCU_ISC,");
        sql.append( " TASA_ISC,");
        sql.append( " DOCU_OTROSTRIBUTOS  as  DOCU_OTROSTRIBUTOS,");
        sql.append( " TASA_OTROSTRIBUTOS,");

        sql.append( " RETE_REGI,");// 01 TASA 3%
        sql.append( " RETE_TASA,"); // 3%           
        sql.append( " RETE_TOTAL_ELEC,"); //
        sql.append( " RETE_TOTAL_RETE,"); //

        sql.append( " DOCU_OTROSCARGOS  as  DOCU_OTROSCARGOS,");
        sql.append( " DOCU_PERCEPCION  as  DOCU_PERCEPCION,");
        sql.append( " NOTA_MOTIVO,");
        sql.append( " NOTA_SUSTENTO,");
        sql.append( " NOTA_TIPODOC,");
        sql.append( " NOTA_DOCUMENTO, ");
        sql.append( " docu_enviaws");
        sql.append( " FROM cabecera");
        sql.append( params.filter(" WHERE  DOCU_CODIGO = :pdocu_codigo",pdocu_codigo));	
		
		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));
		
		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}		
		
		return null;
	}

	@Override
	public List<DocumentoBean> cargarDetDocElectronico(String pdocu_codigo) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean yaExisteDocElectronico(DocumentoBean pItems) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DocumentoBean noPendienteDocElectronico() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentoBean insertarDocElectronico(DocumentoBean pItems, List<DocumentoBean> plItems) {
		// TODO Auto-generated method stub
		return null;
	}

}

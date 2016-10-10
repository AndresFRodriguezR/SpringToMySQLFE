package pe.joedayz.etl.dispatcher.impl;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import pe.joedayz.etl.beans.DocumentoBean;
import pe.joedayz.etl.dispatcher.ResumenBajaDespachador;
import pe.joedayz.etl.support.WhereParams;


@Repository
public class ResumenBajaDespachadorImpl implements ResumenBajaDespachador {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(ResumenBajaDespachadorImpl.class);

	@Autowired
	@Qualifier("mysqlDataSource")
	DataSource dataSource;

	private NamedParameterJdbcTemplate jdbcTemplate;

	@PostConstruct
	public void init() {
		jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	
	@Override
	public DocumentoBean cargarCabecera(String trans) {

		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		
        sql.append("SELECT EMPR_RAZONSOCIAL,");
        sql.append(" EMPR_NRORUC,");
        sql.append(" RESU_FECHA_DOC,");
        sql.append(" RESU_IDENTIFICADOR,");
        sql.append(" RESU_FECHA_COM"); //YYYY-MM-DD
        sql.append(" FROM RESUMENDIA_BAJA");
        sql.append(params.filter(" WHERE CODIGO  = :codigo ", trans));
		
		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}

		
		
		return null;
	}

	@Override
	public List<DocumentoBean> cargarResumen(String trans) throws Exception {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();

		sql.append("SELECT  RESU_FECHA,");
		sql.append(" RESU_FILA, ");
		sql.append(" RESU_TIPODOC, ");
		sql.append(" RESU_SERIE,");
		sql.append(" RESU_NUMERO, ");
		sql.append(" RESU_MOTIVO");
		sql.append(" FROM  COMUNICABAJA");
		sql.append(params.filter(" WHERE CODIGO  = :codigo ", trans));
		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		
		return searchResults;
	}

}

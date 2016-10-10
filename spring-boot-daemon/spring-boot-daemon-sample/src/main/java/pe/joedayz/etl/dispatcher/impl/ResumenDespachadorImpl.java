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
import pe.joedayz.etl.dispatcher.ResumenDespachador;
import pe.joedayz.etl.support.WhereParams;

@Repository
public class ResumenDespachadorImpl implements ResumenDespachador {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(ResumenDespachadorImpl.class);

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
		sql.append("SELECT ");
		sql.append(" EMPR_RAZONSOCIAL,");
		sql.append(" EMPR_NRORUC,");
		sql.append(" RESU_FECHA,");
		sql.append(" RESU_IDENTIFICADOR,");
		sql.append(" RESU_FEC,");
		sql.append(" NROTICKET");
		sql.append(" FROM RESUMENDIA_CAB ");//estos serian los campos ok
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
        sql.append("SELECT RESU_FECHA,");
        sql.append(" RESU_FILA,");
        sql.append(" RESU_TIPODOC,");
        sql.append(" RESU_SERIE,");
        sql.append(" RESU_INICIO, ");
        sql.append(" RESU_FINAL,");
        sql.append(" RESU_GRAVADA,");
        sql.append(" RESU_EXONERADA,");
        sql.append(" RESU_INAFECTA,");
        sql.append(" RESU_OTCARGOS,");
        sql.append(" RESU_ISC,");
        sql.append(" RESU_IGV,");
        sql.append(" RESU_OTTRIBUTOS,");
        sql.append(" RESU_TOTAL");
        sql.append(params.filter(" WHERE CODIGO  = :codigo ", trans));
        
		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		
		return searchResults;
	}

}

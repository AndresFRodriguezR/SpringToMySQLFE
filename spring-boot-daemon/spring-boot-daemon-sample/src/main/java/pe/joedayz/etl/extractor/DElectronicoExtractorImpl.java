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
	public DocumentoBean cargarDEFactura(DocumentoBean documentoBean) {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append("RTRIM(LTRIM(P.NombreCompleto)) EMPR_RAZONSOCIAL, ");
		sql.append("  ");
		sql.append("( ");
		sql.append("   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia))+RTRIM(LTRIM(T.CodigoPostal)) ");
		sql.append("                                                                FROM CO_FiscalEstablecimiento T ");
		sql.append(
				"                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append(
				"                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append(
				"                                                                      T.Estado = 'A') EMPR_UBIGEO, ");
		sql.append("  ");
		sql.append("RTRIM(LTRIM(P.NombreCompleto)) EMPR_NOMBRECOMERCIAL, ");
		sql.append("  ");
		sql.append("( SELECT RTRIM(LTRIM(T.Direccion)) FROM CO_FiscalEstablecimiento T ");
		sql.append("WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append("      T.Estado = 'A' ) EMPR_DIRECCION , ");
		sql.append("  ");
		sql.append("( SELECT RTRIM(LTRIM(Pr.DescripcionCorta)) ");
		sql.append("    FROM dbo.Provincia Pr ");
		sql.append("   WHERE RTRIM(LTRIM(Pr.Departamento))+RTRIM(LTRIM(Pr.Provincia)) in ( ");
		sql.append("   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia)) FROM CO_FiscalEstablecimiento T ");
		sql.append(
				"                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append(
				"                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append("                                                                      T.Estado = 'A' ");
		sql.append("   )) EMPR_PROVINCIA , ");
		sql.append("         ");
		sql.append("( SELECT RTRIM(LTRIM(D.DescripcionCorta)) ");
		sql.append("    FROM dbo.Departamento D ");
		sql.append(
				"   WHERE RTRIM(LTRIM(D.Departamento)) in ( SELECT RTRIM(LTRIM(T.Departamento)) FROM CO_FiscalEstablecimiento T ");
		sql.append(
				"                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append(
				"                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append("                                                                      T.Estado = 'A' ) AND ");
		sql.append("         D.Pais = 'PER') EMPR_DEPARTAMENTO , ");
		sql.append("         ");
		sql.append("( SELECT RTRIM(LTRIM(Z.DescripcionCorta)) ");
		sql.append("    FROM ZonaPostal Z ");
		sql.append("   WHERE RTRIM(LTRIM(Z.Departamento))+ ");
		sql.append("         RTRIM(LTRIM(Z.Provincia))+   ");
		sql.append("         RTRIM(LTRIM(Z.CodigoPostal)) IN ( ");
		sql.append("   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia))+RTRIM(LTRIM(T.CodigoPostal)) ");
		sql.append("                                                                FROM CO_FiscalEstablecimiento T ");
		sql.append(
				"                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append(
				"                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append("                                                                      T.Estado = 'A') ");
		sql.append("         ) EMPR_DISTRITO, ");
		sql.append("         ");
		sql.append("'PE' EMPR_PAIS, ");
		sql.append("RTRIM(LTRIM(P.DocumentoFiscal)) EMPR_NRORUC, ");
		sql.append("'6' EMPR_TIPODOC , ");
		sql.append("RTRIM(LTRIM(A.ClienteRUC)) CLIE_NUMERO, ");
		sql.append("'6' CLIE_TIPODOC, ");
		sql.append("RTRIM(LTRIM(A.ClienteNombre)) CLIE_NOMBRE, ");
		sql.append("CONVERT(CHAR(10),A.FechaDocumento,126) DOCU_FECHA, ");
		sql.append("( SELECT Tdoc.CodigoFiscal ");
		sql.append("    FROM CO_TipoDocumento Tdoc ");
		sql.append("   WHERE Tdoc.TipoDocumento = A.TipoDocumento ) DOCU_TIPODOCUMENTO , ");
		sql.append("   A.NumeroDocumento DOCU_NUMERO , ");
		sql.append("   CASE A.MonedaDocumento ");
		sql.append("   WHEN 'LO' THEN 'PEN' ");
		sql.append("   WHEN 'EX' THEN 'USD' ");
		sql.append("   END DOCU_MONEDA , ");
		sql.append("convert(decimal(15,2),A.MontoAfecto) DOCU_GRAVADA, ");
		sql.append("convert(decimal(15,2),A.MontoNoAfecto) DOCU_INAFECTA, ");
		sql.append("0 DOCU_EXONERADA, ");
		sql.append("0 DOCU_GRATUITA, ");
		sql.append("0 DOCU_DESCUENTO, ");
		sql.append(
				"convert(decimal(15,2),( COALESCE(A.MontoAfecto,0) sql.append( COALESCE(A.MontoNoAfecto,0))) DOCU_SUBTOTAL, ");
		sql.append("convert(decimal(15,2),A.MontoTotal) DOCU_TOTAL, ");
		sql.append("convert(decimal(15,2),A.MontoImpuestoVentas) DOCU_IGV, ");
		sql.append("convert(decimal(15,2),A.TransferenciaGratuitaIGVFactor) TASA_IGV, ");
		sql.append("0 DOCU_ISC, ");
		sql.append("0 TASA_ISC, ");
		sql.append("0 DOCU_OTROSTRIBUTOS, ");
		sql.append("0 TASA_OTROSTRIBUTOS, ");
		sql.append("0 DOCU_OTROSCARGOS, ");
		sql.append("0 DOCU_PERCEPCION, ");
		sql.append("' ' NOTA_MOTIVO,  ");
		sql.append("' ' NOTA_SUSTENTO,  ");
		sql.append("' ' NOTA_TIPODOC,  ");
		sql.append("' ' NOTA_DOCUMENTO, ");
		sql.append("                    ");
		sql.append("( SELECT RTRIM(LTRIM(i.Descripcion)) ");
		sql.append("    FROM MA_FormadePago i ");
		sql.append("   WHERE i.FormadePago = A.FormadePago ) FORMA_PAGO , "); // nueva
																				// columna
		sql.append("  ");
		sql.append("RTRIM(LTRIM(A.Comentarios)) OBSERVACION , "); // nueva
																	// columna
		sql.append("  ");
		sql.append("UPPER(RTRIM(LTRIM(A.ClienteDireccion))) DIRECCION_CLIENTE , ");// nueva
																					// columna
		sql.append("  ");
		sql.append("( SELECT RTRIM(LTRIM(UPPER(V.NombreCompleto))) ");
		sql.append("    FROM PersonaMast V ");
		sql.append("   WHERE V.Persona = A.Vendedor ) VENDEDOR , ");// nueva
																	// columna
		sql.append("  ");
		sql.append("( SELECT MAX(RTRIM(LTRIM(gr.ReferenciaNumeroPedido))) ");
		sql.append("    FROM WH_GuiaRemision gr  ");
		sql.append("   WHERE A.CompaniaSocio=gr.CompaniaSocio and ");
		sql.append("         rtrim(A.TipoDocumento)+rtrim(A.NumeroDocumento)=gr.FacturaNumero AND ");
		sql.append("         gr.Estado <> 'AN'         ) PEDIDO, "); // nueva
																		// columna
		sql.append("   ");
		sql.append("( SELECT MAX(RTRIM(gr.SerieNumero)+'-'+RTRIM(gr.GuiaNumero) ) ");
		sql.append("    FROM WH_GuiaRemision gr ");
		sql.append("   WHERE gr.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("         gr.FacturaNumero = RTRIM(A.TipoDocumento)+RTRIM(A.NumeroDocumento) AND ");
		sql.append("         gr.Estado <> 'AN'         ) GUIA_REMISION, ");// nueva
																			// columna
		sql.append("  ");
		sql.append("( SELECT RTRIM(LTRIM(PE.ClienteReferencia)) ");
		sql.append("    FROM CO_DOCUMENTO PE ");
		sql.append("   WHERE PE.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("         PE.TipoDocumento = 'PE' AND ");
		sql.append("         PE.NumeroDocumento IN ( SELECT MAX(RTRIM(LTRIM(gr.ReferenciaNumeroPedido))) ");
		sql.append("                                    FROM WH_GuiaRemision gr ");
		sql.append("                                   WHERE A.CompaniaSocio=gr.CompaniaSocio and ");
		sql.append(
				"                                         rtrim(A.TipoDocumento)+rtrim(A.NumeroDocumento)=gr.FacturaNumero AND ");
		sql.append("                                         gr.Estado <> 'AN' ) AND ");
		sql.append("         PE.Estado <> 'AN'         ) ORDEN_COMPRA ");// nueva
																			// columna
		sql.append("  ");
		sql.append("FROM PERSONAMAST P , CO_DOCUMENTO A   ");
		sql.append(params.filter("WHERE ( P.Persona in (SELECT CASE :clie_numero  ", documentoBean.getClie_numero()));
		sql.append("                       WHEN '00000100' THEN 14815 ");
		sql.append("                       WHEN '00000200' THEN 14524 ");
		sql.append("                       WHEN '00000300' THEN 17488 ");
		sql.append("                       END) ) AND  ");
		sql.append(params.filter("      ( A.tipodocumento = :docu_tipodocumento ) AND   ",
				documentoBean.getDocu_tipodocumento())); // FC
		sql.append(params.filter("      ( A.NumeroDocumento = :docu_numero  ) AND ", documentoBean.getDocu_numero()));
		// sql.append( " ( YEAR(A.fechadocumento) = 2016 ) AND "
		sql.append("      (A.Estado <> 'AN' ) ");
		sql.append("ORDER BY A.NumeroDocumento");

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}

		return null;
	}

	@Override
	public DocumentoBean cargarDEBoleta(DocumentoBean documentoBean) {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
        sql.append( "RTRIM(LTRIM(P.NombreCompleto)) EMPR_RAZONSOCIAL, ");
        sql.append( "( ");
        sql.append( "   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia))+RTRIM(LTRIM(T.CodigoPostal)) ");
        sql.append( "                                                                FROM CO_FiscalEstablecimiento T ");
        sql.append( "                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
        sql.append( "                                                                      T.Estado = 'A') EMPR_UBIGEO, ");
        sql.append( "RTRIM(LTRIM(P.NombreCompleto)) EMPR_NOMBRECOMERCIAL, ");
        sql.append( "( SELECT RTRIM(LTRIM(T.Direccion)) FROM CO_FiscalEstablecimiento T ");
        sql.append( "WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
        sql.append( "      T.Estado = 'A' ) EMPR_DIRECCION , ");
        sql.append( "                    ");
        sql.append( "( SELECT RTRIM(LTRIM(Pr.DescripcionCorta)) ");
        sql.append( "    FROM dbo.Provincia Pr ");
        sql.append( "   WHERE RTRIM(LTRIM(Pr.Departamento))+RTRIM(LTRIM(Pr.Provincia)) in ( ");
        sql.append( "   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia)) FROM CO_FiscalEstablecimiento T ");
        sql.append( "                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
        sql.append( "                                                                      T.Estado = 'A' ");
        sql.append( "   )) EMPR_PROVINCIA , ");
        sql.append( "         ");
        sql.append( "( SELECT RTRIM(LTRIM(D.DescripcionCorta)) ");
        sql.append( "    FROM dbo.Departamento D ");
        sql.append( "   WHERE RTRIM(LTRIM(D.Departamento)) in ( SELECT RTRIM(LTRIM(T.Departamento)) FROM CO_FiscalEstablecimiento T ");
        sql.append( "                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
        sql.append( "                                                                      T.Estado = 'A' ) AND ");
        sql.append( "         D.Pais = 'PER') EMPR_DEPARTAMENTO , ");
        sql.append( "         ");
        sql.append( "( SELECT RTRIM(LTRIM(Z.DescripcionCorta)) ");
        sql.append( "    FROM ZonaPostal Z ");
        sql.append( "   WHERE RTRIM(LTRIM(Z.Departamento))+ ");
        sql.append( "         RTRIM(LTRIM(Z.Provincia))+   ");
        sql.append( "         RTRIM(LTRIM(Z.CodigoPostal)) IN ( ");
        sql.append( "   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia))+RTRIM(LTRIM(T.CodigoPostal)) ");
        sql.append( "                                                                FROM CO_FiscalEstablecimiento T ");
        sql.append( "                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
        sql.append( "                                                                      T.Estado = 'A') ");
        sql.append( "         ) EMPR_DISTRITO, ");
        sql.append( "                    ");
        sql.append( "'PE' EMPR_PAIS, ");
        sql.append( "RTRIM(LTRIM(P.DocumentoFiscal)) EMPR_NRORUC, ");
        sql.append( "'6' EMPR_TIPODOC , ");
        sql.append( "RTRIM(LTRIM(A.ClienteRUC)) CLIE_NUMERO, ");
        sql.append( "CASE LEN(RTRIM(LTRIM(A.ClienteRUC))) ");
        sql.append( "                    WHEN 8 THEN '1' ");
        sql.append( "                    WHEN 11 THEN '6' ");
        sql.append( "                    ELSE '0' ");
        sql.append( "END CLIE_TIPODOC, ");
        sql.append( "  ");
        sql.append( "RTRIM(LTRIM(A.ClienteNombre)) CLIE_NOMBRE, ");
        sql.append( "CONVERT(CHAR(10),A.FechaDocumento,126) DOCU_FECHA, ");
        sql.append( "( SELECT Tdoc.CodigoFiscal ");
        sql.append( "    FROM CO_TipoDocumento Tdoc ");
        sql.append( "   WHERE Tdoc.TipoDocumento = A.TipoDocumento ) DOCU_TIPODOCUMENTO , ");
        sql.append( "   A.NumeroDocumento DOCU_NUMERO , ");
        sql.append( "   CASE A.MonedaDocumento ");
        sql.append( "   WHEN 'LO' THEN 'PEN' ");
        sql.append( "   WHEN 'EX' THEN 'USD' ");
        sql.append( "   END DOCU_MONEDA , ");
        sql.append( "  ");
        sql.append( "CONVERT(DECIMAL(15,2),A.MontoAfecto) DOCU_GRAVADA, ");
        sql.append( "CONVERT(DECIMAL(15,2),A.MontoNoAfecto) DOCU_INAFECTA, ");
        sql.append( "  ");
        sql.append( "0.0 DOCU_EXONERADA, ");
        sql.append( "0.00 DOCU_GRATUITA, ");
        sql.append( "0.00 DOCU_DESCUENTO, ");
        sql.append( "convert(decimal(15,2),( COALESCE(A.MontoAfecto,0) sql.append( COALESCE(A.MontoNoAfecto,0))) DOCU_SUBTOTAL, ");
        sql.append( "convert(decimal(15,2),A.MontoTotal) DOCU_TOTAL, ");
        sql.append( "convert(decimal(15,2),A.MontoImpuestoVentas) DOCU_IGV, ");
        sql.append( "convert(decimal(15,2),A.TransferenciaGratuitaIGVFactor) TASA_IGV, ");
        sql.append( "  ");
        sql.append( "0.00 DOCU_ISC, ");
        sql.append( "0.00 TASA_ISC, ");
        sql.append( "0.00 DOCU_OTROSTRIBUTOS, ");
        sql.append( "0.00 TASA_OTROSTRIBUTOS, ");
        sql.append( "0.00 DOCU_OTROSCARGOS, ");
        sql.append( "0.00 DOCU_PERCEPCION, ");
        sql.append( "' ' NOTA_MOTIVO,  ");
        sql.append( "' ' NOTA_SUSTENTO,  ");
        sql.append( "' ' NOTA_TIPODOC,  ");
        sql.append( "' ' NOTA_DOCUMENTO, ");
        sql.append( "                    ");
        sql.append( "( SELECT RTRIM(LTRIM(i.Descripcion)) ");
        sql.append( "    FROM MA_FormadePago i ");
        sql.append( "   WHERE i.FormadePago = A.FormadePago ) FORMA_PAGO , ");
        sql.append( "  ");
        sql.append( "RTRIM(LTRIM(A.NotaCreditoDocumento)) DOC_REF, ");
        sql.append( "  ");
        sql.append( "  ");
        sql.append( "( SELECT CONVERT(VARCHAR,NC.FechaDocumento,111) ");
        sql.append( " FROM CO_DOCUMENTO NC ");
        sql.append( "WHERE NC.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "      NC.TipoDocumento = SUBSTRING(A.NotaCreditoDocumento,1,2) AND ");
        sql.append( "      NC.NumeroDocumento = SUBSTRING(A.NotaCreditoDocumento,4,11) AND ");
        sql.append( "      NC.Estado <> 'AN') DOC_REF_FECHA , ");
        sql.append( "  ");
        sql.append( "RTRIM(LTRIM(A.Comentarios)) OBSERVACION , ");
        sql.append( "  ");
        sql.append( "UPPER(RTRIM(LTRIM(A.ClienteDireccion))) DIRECCION_CLIENTE , ");
        sql.append( "  ");
        sql.append( "( SELECT RTRIM(LTRIM(UPPER(V.NombreCompleto))) ");
        sql.append( "    FROM PersonaMast V ");
        sql.append( "   WHERE V.Persona = A.Vendedor ) VENDEDOR , ");
        sql.append( "  ");
        sql.append( "( SELECT MAX(RTRIM(LTRIM(gr.ReferenciaNumeroPedido))) ");
        sql.append( "    FROM WH_GuiaRemision gr  ");
        sql.append( "   WHERE A.CompaniaSocio=gr.CompaniaSocio and ");
        sql.append( "         rtrim(A.TipoDocumento)+rtrim(A.NumeroDocumento)=gr.FacturaNumero AND ");
        sql.append( "         gr.Estado <> 'AN'         ) PEDIDO, ");
        sql.append( "   ");
        sql.append( "( SELECT MAX(RTRIM(gr.SerieNumero)+'-'+RTRIM(gr.GuiaNumero) ) ");
        sql.append( "    FROM WH_GuiaRemision gr ");
        sql.append( "   WHERE gr.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "         gr.FacturaNumero = RTRIM(A.TipoDocumento)+RTRIM(A.NumeroDocumento) AND ");
        sql.append( "         gr.Estado <> 'AN'         ) GUIA_REMISION, ");
        sql.append( "  ");
        sql.append( "( SELECT RTRIM(LTRIM(PE.ClienteReferencia)) ");
        sql.append( "    FROM CO_DOCUMENTO PE ");
        sql.append( "   WHERE PE.CompaniaSocio = A.CompaniaSocio AND ");
        sql.append( "         PE.TipoDocumento = 'PE' AND ");
        sql.append( "         PE.NumeroDocumento IN ( SELECT MAX(RTRIM(LTRIM(gr.ReferenciaNumeroPedido))) ");
        sql.append( "                                    FROM WH_GuiaRemision gr ");
        sql.append( "                                   WHERE A.CompaniaSocio=gr.CompaniaSocio and ");
        sql.append( "                                         rtrim(A.TipoDocumento)+rtrim(A.NumeroDocumento)=gr.FacturaNumero AND ");
        sql.append( "                                         gr.Estado <> 'AN' ) AND ");
        sql.append( "         PE.Estado <> 'AN'         ) ORDEN_COMPRA ");
        sql.append( "  ");
        sql.append( "  ");
        sql.append( "  ");
        sql.append( "FROM PERSONAMAST P , CO_DOCUMENTO A  ");
        //                    sql.append( "WHERE ( P.Persona = 14524 ) AND "
        //                    sql.append( "      ( A.companiasocio = '00000200' ) AND  "
        //                    sql.append( "      ( A.tipodocumento = 'BV' ) AND  "
        //                    sql.append( "      ( YEAR(A.fechadocumento) = 2016 ) AND "
        //                    sql.append( "      A.Estado <> 'AN' "
        //                    sql.append( "ORDER BY A.NumeroDocumento "
		sql.append(params.filter("WHERE ( P.Persona in (SELECT CASE :clie_numero  ", documentoBean.getClie_numero()));
        sql.append( "                       WHEN '00000100' THEN 14815 ");
        sql.append( "                       WHEN '00000200' THEN 14524 ");
        sql.append( "                       WHEN '00000300' THEN 17488 ");
        sql.append( "                       END) ) AND  ");
		sql.append(params.filter("      ( A.tipodocumento = :docu_tipodocumento ) AND   ",
				documentoBean.getDocu_tipodocumento())); // FC
		sql.append(params.filter("      ( A.NumeroDocumento = :docu_numero  ) AND ", documentoBean.getDocu_numero()));
        //                    sql.append( "      ( YEAR(A.fechadocumento) = 2016 ) AND "
        sql.append( "      (A.Estado <> 'AN' ) ");
        sql.append( "ORDER BY A.NumeroDocumento");

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}
		
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

	private static final String SEARCH_DOCUMENTS = "SELECT TOP 1 \n" + "	companiasocio as clie_numero, \n"
			+ " tipodocumento as docu_tipodocumento, \n" + "numerodocumento as docu_numero \n" + " FROM efactura \n"
			+ " WHERE  nuevo = 'N' \n" + " order by FechaDocumento ";

	@Override
	public DocumentoBean pendienteDocElectronico() {

		Map<String, String> queryParams = new HashMap<>();

		List<DocumentoBean> searchResults = jdbcTemplate.query(SEARCH_DOCUMENTS, queryParams,
				new BeanPropertyRowMapper<>(DocumentoBean.class));

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
		sql.append(params.filter("efactura set nuevo='P' where companiasocio= :clie_numero ",
				documentoBean.getClie_numero()));
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

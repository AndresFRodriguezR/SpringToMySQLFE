package pe.joedayz.etl.extractor;

import java.sql.SQLException;
import java.util.ArrayList;
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
		sql.append("RTRIM(LTRIM(P.NombreCompleto)) EMPR_RAZONSOCIAL, ");
		sql.append("( ");
		sql.append("   SELECT RTRIM(LTRIM(T.Departamento))+RTRIM(LTRIM(T.Provincia))+RTRIM(LTRIM(T.CodigoPostal)) ");
		sql.append("                                                                FROM CO_FiscalEstablecimiento T ");
		sql.append(
				"                                                                WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append(
				"                                                                      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append(
				"                                                                      T.Estado = 'A') EMPR_UBIGEO, ");
		sql.append("RTRIM(LTRIM(P.NombreCompleto)) EMPR_NOMBRECOMERCIAL, ");
		sql.append("( SELECT RTRIM(LTRIM(T.Direccion)) FROM CO_FiscalEstablecimiento T ");
		sql.append("WHERE T.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("      T.EstablecimientoCodigo = A.EstablecimientoCodigo AND ");
		sql.append("      T.Estado = 'A' ) EMPR_DIRECCION , ");
		sql.append("                    ");
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
		sql.append("                    ");
		sql.append("'PE' EMPR_PAIS, ");
		sql.append("RTRIM(LTRIM(P.DocumentoFiscal)) EMPR_NRORUC, ");
		sql.append("'6' EMPR_TIPODOC , ");
		sql.append("RTRIM(LTRIM(A.ClienteRUC)) CLIE_NUMERO, ");
		sql.append("CASE LEN(RTRIM(LTRIM(A.ClienteRUC))) ");
		sql.append("                    WHEN 8 THEN '1' ");
		sql.append("                    WHEN 11 THEN '6' ");
		sql.append("                    ELSE '0' ");
		sql.append("END CLIE_TIPODOC, ");
		sql.append("  ");
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
		sql.append("  ");
		sql.append("CONVERT(DECIMAL(15,2),A.MontoAfecto) DOCU_GRAVADA, ");
		sql.append("CONVERT(DECIMAL(15,2),A.MontoNoAfecto) DOCU_INAFECTA, ");
		sql.append("  ");
		sql.append("0.0 DOCU_EXONERADA, ");
		sql.append("0.00 DOCU_GRATUITA, ");
		sql.append("0.00 DOCU_DESCUENTO, ");
		sql.append(
				"convert(decimal(15,2),( COALESCE(A.MontoAfecto,0) sql.append( COALESCE(A.MontoNoAfecto,0))) DOCU_SUBTOTAL, ");
		sql.append("convert(decimal(15,2),A.MontoTotal) DOCU_TOTAL, ");
		sql.append("convert(decimal(15,2),A.MontoImpuestoVentas) DOCU_IGV, ");
		sql.append("convert(decimal(15,2),A.TransferenciaGratuitaIGVFactor) TASA_IGV, ");
		sql.append("  ");
		sql.append("0.00 DOCU_ISC, ");
		sql.append("0.00 TASA_ISC, ");
		sql.append("0.00 DOCU_OTROSTRIBUTOS, ");
		sql.append("0.00 TASA_OTROSTRIBUTOS, ");
		sql.append("0.00 DOCU_OTROSCARGOS, ");
		sql.append("0.00 DOCU_PERCEPCION, ");
		sql.append("' ' NOTA_MOTIVO,  ");
		sql.append("' ' NOTA_SUSTENTO,  ");
		sql.append("' ' NOTA_TIPODOC,  ");
		sql.append("' ' NOTA_DOCUMENTO, ");
		sql.append("                    ");
		sql.append("( SELECT RTRIM(LTRIM(i.Descripcion)) ");
		sql.append("    FROM MA_FormadePago i ");
		sql.append("   WHERE i.FormadePago = A.FormadePago ) FORMA_PAGO , ");
		sql.append("  ");
		sql.append("RTRIM(LTRIM(A.NotaCreditoDocumento)) DOC_REF, ");
		sql.append("  ");
		sql.append("  ");
		sql.append("( SELECT CONVERT(VARCHAR,NC.FechaDocumento,111) ");
		sql.append(" FROM CO_DOCUMENTO NC ");
		sql.append("WHERE NC.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("      NC.TipoDocumento = SUBSTRING(A.NotaCreditoDocumento,1,2) AND ");
		sql.append("      NC.NumeroDocumento = SUBSTRING(A.NotaCreditoDocumento,4,11) AND ");
		sql.append("      NC.Estado <> 'AN') DOC_REF_FECHA , ");
		sql.append("  ");
		sql.append("RTRIM(LTRIM(A.Comentarios)) OBSERVACION , ");
		sql.append("  ");
		sql.append("UPPER(RTRIM(LTRIM(A.ClienteDireccion))) DIRECCION_CLIENTE , ");
		sql.append("  ");
		sql.append("( SELECT RTRIM(LTRIM(UPPER(V.NombreCompleto))) ");
		sql.append("    FROM PersonaMast V ");
		sql.append("   WHERE V.Persona = A.Vendedor ) VENDEDOR , ");
		sql.append("  ");
		sql.append("( SELECT MAX(RTRIM(LTRIM(gr.ReferenciaNumeroPedido))) ");
		sql.append("    FROM WH_GuiaRemision gr  ");
		sql.append("   WHERE A.CompaniaSocio=gr.CompaniaSocio and ");
		sql.append("         rtrim(A.TipoDocumento)+rtrim(A.NumeroDocumento)=gr.FacturaNumero AND ");
		sql.append("         gr.Estado <> 'AN'         ) PEDIDO, ");
		sql.append("   ");
		sql.append("( SELECT MAX(RTRIM(gr.SerieNumero)+'-'+RTRIM(gr.GuiaNumero) ) ");
		sql.append("    FROM WH_GuiaRemision gr ");
		sql.append("   WHERE gr.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("         gr.FacturaNumero = RTRIM(A.TipoDocumento)+RTRIM(A.NumeroDocumento) AND ");
		sql.append("         gr.Estado <> 'AN'         ) GUIA_REMISION, ");
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
		sql.append("         PE.Estado <> 'AN'         ) ORDEN_COMPRA ");
		sql.append("  ");
		sql.append("  ");
		sql.append("  ");
		sql.append("FROM PERSONAMAST P , CO_DOCUMENTO A  ");
		// sql.append( "WHERE ( P.Persona = 14524 ) AND "
		// sql.append( " ( A.companiasocio = '00000200' ) AND "
		// sql.append( " ( A.tipodocumento = 'BV' ) AND "
		// sql.append( " ( YEAR(A.fechadocumento) = 2016 ) AND "
		// sql.append( " A.Estado <> 'AN' "
		// sql.append( "ORDER BY A.NumeroDocumento "
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
	public DocumentoBean cargarDENCredito(DocumentoBean documentoBean) {
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
		sql.append("CASE LEN(RTRIM(LTRIM(A.ClienteRUC))) ");
		sql.append("                    WHEN 8 THEN '1' ");
		sql.append("                    WHEN 11 THEN '6' ");
		sql.append("                    ELSE '0' ");
		sql.append("END CLIE_TIPODOC, ");
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
		// sql.append( "CASE A.TipoDocumento "
		// sql.append( "WHEN 'NC' THEN convert(decimal(15,2),A.MontoAfecto) * -1
		// "
		// sql.append( "ELSE convert(decimal(15,2),A.MontoAfecto) "
		// sql.append( "END DOCU_GRAVADA, "
		sql.append("abs(convert(decimal(15,2),A.MontoAfecto)) DOCU_GRAVADA, ");
		// sql.append( "CASE A.TipoDocumento "
		// sql.append( "WHEN 'NC' THEN convert(decimal(15,2),A.MontoNoAfecto) *
		// -1 "
		// sql.append( "ELSE convert(decimal(15,2),A.MontoNoAfecto) "
		// sql.append( "END DOCU_INAFECTA, "
		sql.append("abs(convert(decimal(15,2),A.MontoNoAfecto)) DOCU_INAFECTA, ");
		sql.append("0 DOCU_EXONERADA, ");
		sql.append("0 DOCU_GRATUITA, ");
		sql.append("0 DOCU_DESCUENTO, ");
		// sql.append( "CASE A.TipoDocumento "
		// sql.append( "WHEN 'NC' THEN convert(decimal(15,2),(
		// COALESCE(A.MontoAfecto,0) sql.append( COALESCE(A.MontoNoAfecto,0))) *
		// -1 "
		// sql.append( "ELSE convert(decimal(15,2),( COALESCE(A.MontoAfecto,0)
		// sql.append( COALESCE(A.MontoNoAfecto,0))) "
		// sql.append( "END DOCU_SUBTOTAL, "
		sql.append(
				"abs(convert(decimal(15,2),( COALESCE(A.MontoAfecto,0) sql.append( COALESCE(A.MontoNoAfecto,0)))) DOCU_SUBTOTAL, ");
		// sql.append( "CASE A.TipoDocumento "
		// sql.append( "WHEN 'NC' THEN convert(decimal(15,2),A.MontoTotal) * -1
		// "
		// sql.append( "ELSE convert(decimal(15,2),A.MontoTotal) "
		// sql.append( "END DOCU_TOTAL, "
		sql.append("abs(convert(decimal(15,2),A.MontoTotal))  DOCU_TOTAL, ");
		// sql.append( "CASE A.TipoDocumento "
		// sql.append( "WHEN 'NC' THEN
		// convert(decimal(15,2),A.MontoImpuestoVentas) * -1 "
		// sql.append( "ELSE convert(decimal(15,2),A.MontoImpuestoVentas) "
		// sql.append( "END DOCU_IGV, "
		sql.append("abs(convert(decimal(15,2),A.MontoImpuestoVentas)) DOCU_IGV, ");
		sql.append("convert(decimal(15,2),A.TransferenciaGratuitaIGVFactor) TASA_IGV, ");
		sql.append("0 DOCU_ISC, ");
		sql.append("0 TASA_ISC, ");
		sql.append("0 DOCU_OTROSTRIBUTOS, ");
		sql.append("0 TASA_OTROSTRIBUTOS, ");
		sql.append("0 DOCU_OTROSCARGOS, ");
		sql.append("0 DOCU_PERCEPCION, ");
		sql.append("' ' NOTA_MOTIVO,  ");
		sql.append("RTRIM(LTRIM(A.Comentarios)) NOTA_SUSTENTO,  ");
		sql.append("( SELECT TT.CodigoFiscal ");
		sql.append("    FROM TipoDocumentoCXP TT ");
		sql.append("   WHERE TT.TipoDocumento = RTRIM(LTRIM(SUBSTRING(A.NotaCreditoDocumento,1,2)))) NOTA_TIPODOC,  ");
		sql.append("   ");
		sql.append("RTRIM(LTRIM(SUBSTRING(A.NotaCreditoDocumento,4,20))) NOTA_DOCUMENTO, ");
		sql.append("                    ");
		sql.append("( SELECT RTRIM(LTRIM(i.Descripcion)) ");
		sql.append("    FROM MA_FormadePago i ");
		sql.append("   WHERE i.FormadePago = A.FormadePago ) FORMA_PAGO , ");
		sql.append("  ");
		sql.append("( SELECT CONVERT(VARCHAR,NC.FechaDocumento,111) ");
		sql.append(" FROM CO_DOCUMENTO NC ");
		sql.append("WHERE NC.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("      NC.TipoDocumento = SUBSTRING(A.NotaCreditoDocumento,1,2) AND ");
		sql.append("      NC.NumeroDocumento = SUBSTRING(A.NotaCreditoDocumento,4,11) AND ");
		sql.append("      NC.Estado <> 'AN') DOC_REF_FECHA , ");
		sql.append("  ");
		sql.append("UPPER(RTRIM(LTRIM(A.ClienteDireccion))) DIRECCION_CLIENTE , ");
		sql.append("  ");
		sql.append("( SELECT RTRIM(LTRIM(UPPER(V.NombreCompleto))) ");
		sql.append("    FROM PersonaMast V ");
		sql.append("   WHERE V.Persona = A.Vendedor ) VENDEDOR , ");
		sql.append("  ");
		sql.append("( SELECT MAX(RTRIM(LTRIM(gr.ReferenciaNumeroPedido))) ");
		sql.append("    FROM WH_GuiaRemision gr  ");
		sql.append("   WHERE A.CompaniaSocio=gr.CompaniaSocio and ");
		sql.append("         rtrim(A.TipoDocumento)+rtrim(A.NumeroDocumento)=gr.FacturaNumero AND ");
		sql.append("         gr.Estado <> 'AN'         ) PEDIDO, ");
		sql.append("   ");
		sql.append("( SELECT MAX(RTRIM(gr.SerieNumero)+'-'+RTRIM(gr.GuiaNumero) ) ");
		sql.append("    FROM WH_GuiaRemision gr ");
		sql.append("   WHERE gr.CompaniaSocio = A.CompaniaSocio AND ");
		sql.append("         gr.FacturaNumero = RTRIM(A.TipoDocumento)+RTRIM(A.NumeroDocumento) AND ");
		sql.append("         gr.Estado <> 'AN'         ) GUIA_REMISION, ");
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
		sql.append("         PE.Estado <> 'AN'         ) ORDEN_COMPRA ");
		sql.append("  ");
		// sql.append( " "
		// sql.append( "FROM PERSONAMAST P , CO_DOCUMENTO A "
		// sql.append( "WHERE ( P.Persona = 14524 ) AND "
		// sql.append( " ( A.companiasocio = '00000200' ) AND "
		// sql.append( " ( A.tipodocumento = 'NC' ) AND "
		// sql.append( " ( YEAR(A.fechadocumento) = 2016 ) AND "
		// sql.append( " A.Estado <> 'AN' "
		// sql.append( "ORDER BY A.NotaCreditoDocumento "
		sql.append("          ");
		sql.append("FROM PERSONAMAST P , CO_DOCUMENTO A   ");
		sql.append(params.filter("WHERE ( P.Persona in (SELECT CASE :clie_numero  ", documentoBean.getClie_numero()));
		sql.append("                       WHEN '00000100' THEN 14815 ");
		sql.append("                       WHEN '00000200' THEN 14524 ");
		sql.append("                       WHEN '00000300' THEN 17488 ");
		sql.append("                       END) ) AND  ");
		sql.append(params.filter("      ( A.tipodocumento = :docu_tipodocumento ) AND   ",
				documentoBean.getDocu_tipodocumento())); // FC
		sql.append(params.filter("      ( A.NumeroDocumento = :docu_numero  ) AND ", documentoBean.getDocu_numero()));
		sql.append("      (A.Estado <> 'AN' )");
		sql.append("ORDER BY A.NumeroDocumento");

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}

		return null;
	}

	@Override
	public DocumentoBean cargarDENDebito(DocumentoBean documentoBean) {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append("RTRIM(LTRIM(P.NombreCompleto)) EMPR_RAZONSOCIAL, ");
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
		sql.append("          ");
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
		sql.append("          ");
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
		sql.append("  ");
		sql.append("'PE' EMPR_PAIS, ");
		sql.append("RTRIM(LTRIM(P.DocumentoFiscal)) EMPR_NRORUC, ");
		sql.append("'6' EMPR_TIPODOC , ");
		sql.append("RTRIM(LTRIM(A.ClienteRUC)) CLIE_NUMERO, ");
		sql.append("CASE LEN(RTRIM(LTRIM(A.ClienteRUC))) ");
		sql.append("WHEN 8 THEN '0' ");
		sql.append("WHEN 11 THEN '6' ");
		sql.append("END CLIE_TIPODOC, ");
		sql.append("RTRIM(LTRIM(A.ClienteNombre)) CLIE_NOMBRE, ");
		sql.append("CONVERT(VARCHAR,A.FechaDocumento,111) DOCU_FECHA, ");
		sql.append("( SELECT Tdoc.CodigoFiscal ");
		sql.append("    FROM CO_TipoDocumento Tdoc ");
		sql.append("   WHERE Tdoc.TipoDocumento = A.TipoDocumento ) DOCU_TIPODOCUMENTO , ");
		sql.append("   A.NumeroDocumento DOCU_NUMERO , ");
		sql.append("   CASE A.MonedaDocumento ");
		sql.append("   WHEN 'LO' THEN 'PEN' ");
		sql.append("   WHEN 'EX' THEN 'USD' ");
		sql.append("   END DOCU_MONEDA , ");
		sql.append("A.MontoAfecto DOCU_GRAVADA, ");
		sql.append("A.MontoNoAfecto DOCU_INAFECTA, ");
		sql.append("0 DOCU_EXONERADA, ");
		sql.append("0 DOCU_GRATUITA, ");
		sql.append("0 DOCU_DESCUENTO, ");
		sql.append("( COALESCE(A.MontoAfecto,0) sql.append( COALESCE(A.MontoNoAfecto,0) ) DOCU_SUBTOTAL, ");
		sql.append("A.MontoTotal DOCU_TOTAL, ");
		sql.append("A.MontoImpuestoVentas DOCU_IGV, ");
		sql.append("A.TransferenciaGratuitaIGVFactor TASA_IGV, ");
		sql.append("0 DOCU_ISC, ");
		sql.append("0 TASA_ISC, ");
		sql.append("0 DOCU_OTROSTRIBUTOS, ");
		sql.append("0 TASA_OTROSTRIBUTOS, ");
		sql.append("0 DOCU_OTROSCARGOS, ");
		sql.append("0 DOCU_PERCEPCION, ");
		sql.append("0 NOTA_MOTIVO, ");
		sql.append("0 NOTA_SUSTENTO, ");
		sql.append("( SELECT Tdoc.CodigoFiscal ");
		sql.append("    FROM CO_TipoDocumento Tdoc ");
		sql.append("   WHERE Tdoc.TipoDocumento = SUBSTRING(A.NOTACREDITODOCUMENTO,1,2 ))  NOTA_TIPODOC, ");
		sql.append("SUBSTRING(A.NOTACREDITODOCUMENTO,4,11 ) NOTA_DOCUMENTO ");
		sql.append("         ");
		// sql.append( "FROM PERSONAMAST P , CO_DOCUMENTO A "
		// sql.append( "WHERE ( P.Persona = 14524 ) AND "
		// sql.append( " ( A.companiasocio = '00000200' ) AND "
		// sql.append( " ( A.tipodocumento = 'ND' ) AND "
		// sql.append( " ( YEAR(A.fechadocumento) = 2016 ) "
		// sql.append( "ORDER BY A.NumeroDocumento "
		sql.append("          ");
		sql.append("FROM PERSONAMAST P , CO_DOCUMENTO A   ");
		sql.append(params.filter("WHERE ( P.Persona in (SELECT CASE :clie_numero  ", documentoBean.getClie_numero()));
		sql.append("                       WHEN '00000100' THEN 14815 ");
		sql.append("                       WHEN '00000200' THEN 14524 ");
		sql.append("                       WHEN '00000300' THEN 17488 ");
		sql.append("                       END) ) AND  ");
		sql.append(params.filter("      ( A.tipodocumento = :docu_tipodocumento ) AND   ",
				documentoBean.getDocu_tipodocumento())); // FC
		sql.append(params.filter("      ( A.NumeroDocumento = :docu_numero  ) AND ", documentoBean.getDocu_numero()));
		sql.append("      (A.Estado <> 'AN' )");
		sql.append("ORDER BY A.NumeroDocumento");

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}

		return null;
	}

	@Override
	public List<DocumentoBean> cargarDetDocElectronico(DocumentoBean documentoBean) throws SQLException {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		
		sql.append("SELECT ");
        sql.append( "( SELECT CASE A.MonedaDocumento  ");
        sql.append( "            WHEN 'LO' THEN 'PEN'   ");
        sql.append( "            WHEN 'EX' THEN 'USD'   ");
        sql.append( "         END  ");
        sql.append( "    FROM CO_DOCUMENTO A  ");
        sql.append( "    WHERE A.CompaniaSocio = B.CompaniaSocio AND  ");
        sql.append( "          A.TipoDocumento = B.TipoDocumento AND  ");
        sql.append( "          A.NumeroDocumento = B.NumeroDocumento ) DOCU_MONEDA , ");
        sql.append( "		   ");
        sql.append( "( SELECT CASE A.MonedaDocumento  ");
        sql.append( "            WHEN 'LO' THEN 'PEN'   ");
        sql.append( "            WHEN 'EX' THEN 'USD'   ");
        sql.append( "            END  ");
        sql.append( "    FROM CO_DOCUMENTO A  ");
        sql.append( "    WHERE A.CompaniaSocio = B.CompaniaSocio AND  ");
        sql.append( "          A.TipoDocumento = B.TipoDocumento AND  ");
        sql.append( "          A.NumeroDocumento = B.NumeroDocumento ) ITEM_MONEDA ,  ");
        sql.append( "  ");
        sql.append( "B.Linea ITEM_ORDEN,   ");
        sql.append( "CASE B.TipoDetalle ");
        sql.append( "    WHEN 'I' THEN ( SELECT M.CodigoFiscalEfact FROM UnidadesMast M WHERE M.UnidadCodigo = B.UnidadCodigo ) ");
        sql.append( "    WHEN 'S' THEN 'ZZ' ");
        sql.append( "END ITEM_UNIDAD ,  ");
        sql.append( "  ");
        sql.append( "ABS(CONVERT(DECIMAL(15,2),B.CantidadPedida)) ITEM_CANTIDAD, ");
        sql.append( "  ");
        sql.append( "B.ItemCodigo ITEM_CODPRODUCTO,  ");
        sql.append( "RTRIM(LTRIM(B.Descripcion)) ITEM_DESCRIPCION,  ");
        sql.append( "CASE RTRIM(LTRIM(B.ItemCodigo)) ");
        sql.append( "    WHEN 'SU0322001' THEN '15' ");
        sql.append( "    WHEN 'SU0130014' THEN '15' ");
        sql.append( "    WHEN '1801' THEN '15' ");
        sql.append( "    ELSE '10' ");
        sql.append( "END ITEM_AFECTACION, ");
        sql.append( "  ");
        sql.append( "ABS(CONVERT(DECIMAL(15,2),CASE WHEN B.PrecioUnitario <> 0 THEN B.PrecioUnitario ELSE B.PrecioUnitarioDoble END)) ITEM_PVENTA,  ");
        sql.append( "  ");
        sql.append( "ABS(CONVERT(DECIMAL(15,2),( B.CantidadPedida * CASE WHEN B.PrecioUnitario <> 0 THEN B.PrecioUnitario ELSE B.PrecioUnitarioDoble END ))) ITEM_TI_SUBTOTAL,  ");
        sql.append( "  ");
        sql.append( "ABS(CONVERT(DECIMAL(15,2),( ( ( CASE WHEN B.PrecioUnitario <> 0 THEN B.PrecioUnitario ELSE B.PrecioUnitarioDoble END *( SELECT A.TransferenciaGratuitaIGVFactor  ");
        sql.append( "                                                  FROM CO_DOCUMENTO A  ");
        sql.append( "                                                 WHERE A.CompaniaSocio = B.CompaniaSocio AND  ");
        sql.append( "                                                       A.TipoDocumento = B.TipoDocumento AND  ");
        sql.append( "                                                       A.NumeroDocumento = B.NumeroDocumento  ");
        sql.append( "                                              ))/100) * B.CantidadPedida ))) ITEM_TI_IGV ");
        sql.append( "  ");
//        sql.append( "FROM CO_DOCUMENTODETALLE B  "
//        sql.append( "WHERE B.CompaniaSocio = '00000200' AND  "
//        sql.append( "B.TipoDocumento  = 'BV' AND  "
//        sql.append( "B.NumeroDocumento LIKE '001%03404' "
//        sql.append( "--B.TipoDetalle = 'S' "
//        sql.append( "ORDER BY 3 "
//        sql.append( "--B.NumeroDocumento LIKE '001%044851<' "
//        sql.append( " "
        sql.append( " FROM CO_DOCUMENTODETALLE B  ");
        sql.append( params.filter(" WHERE B.CompaniaSocio = :clie_numero AND  ", documentoBean.getClie_numero()));
        sql.append( params.filter("       B.TipoDocumento  = :docu_tipodocumento AND  ", documentoBean.getDocu_tipodocumento()));
        sql.append( params.filter("       B.NumeroDocumento = :docu_numero ", documentoBean.getDocu_numero()));
        sql.append( " ");
        
		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		for(DocumentoBean documento: searchResults){
			List<OtrosDetalles> detadeta = new ArrayList<OtrosDetalles>();
			
			detadeta = cargarDetDocElectronicoDet(documento, documento.getItem_codproducto());
            if (detadeta != null) {
            	documento.setOtrosDetalles(detadeta);
            }
		}
		return searchResults;
	}

	@Override
	public List<OtrosDetalles> cargarDetDocElectronicoDet(DocumentoBean documentoBean, String codProducto) throws SQLException {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		
		sql.append("SELECT RTRIM(LTRIM(WH_TransaccionSerie.NumeroSerie)) Motor, ");
        sql.append( "          RTRIM(LTRIM(WH_ItemSerie.NumeroSerieComponente)) Chasis,           ");
        sql.append( "          (SELECT RTRIM(LTRIM(T.DescripcionCorta)) ");
        sql.append( "            FROM dbo.ColorMast T ");
        sql.append( "            WHERE T.Color = WH_ItemSerie.Color) Color , ");
        sql.append( "          RTRIM(LTRIM(WH_ItemSerie.PeriodoFabricacion)) Fabricacion,           ");
        sql.append( "          RTRIM(LTRIM(WH_Marcas.DescripcionLocal)) Marca,           ");
        sql.append( "          RTRIM(LTRIM(WH_ItemSerie.ModeloAno)) Modelo_Periodo,           ");
        sql.append( "          RTRIM(LTRIM(WH_Modelo.DescripcionLocal)) Modelo  ");
        sql.append( "  FROM WH_GuiaRemision ,           ");
        sql.append( "          WH_GuiaRemisionDetalle ,           ");
        sql.append( "          WH_TransaccionSerie ,           ");
        sql.append( "          WH_ItemSerie ,           ");
        sql.append( "          WH_ItemMast ,           ");
        sql.append( "          WH_Marcas ,           ");
        sql.append( "          WH_Modelo     ");
        sql.append( "  WHERE ( WH_GuiaRemisionDetalle.CompaniaSocio = WH_GuiaRemision.CompaniaSocio ) and          ");
        sql.append( "  ( WH_GuiaRemisionDetalle.SerieNumero = WH_GuiaRemision.SerieNumero ) and          ");
        sql.append( "  ( WH_GuiaRemisionDetalle.GuiaNumero = WH_GuiaRemision.GuiaNumero ) and          ");
        sql.append( "  ( WH_GuiaRemisionDetalle.CompaniaSocio = WH_TransaccionSerie.CompaniaSocio ) and ");
        sql.append( "  ( WH_GuiaRemisionDetalle.ItemCodigo = WH_TransaccionSerie.Item ) and          ");
        sql.append( "  ( WH_GuiaRemisionDetalle.ReferenciaTipoDocumento = WH_TransaccionSerie.TipoDocumento ) and          ");
        sql.append( "  ( WH_GuiaRemisionDetalle.ReferenciaNumeroDocumento = WH_TransaccionSerie.NumeroDocumento ) and          ");
        sql.append( "  ( WH_TransaccionSerie.Item = WH_ItemSerie.Item ) and          ");
        sql.append( "  ( WH_TransaccionSerie.NumeroSerie = WH_ItemSerie.NumeroSerie ) and          ");
        sql.append( "  ( WH_ItemMast.Item = WH_ItemSerie.Item ) and          ");
        sql.append( "  ( WH_Marcas.MarcaCodigo = WH_ItemMast.MarcaCodigo ) and           ");
        sql.append( "  ( WH_ItemMast.Modelo = WH_Modelo.Modelo ) and          ");
        sql.append( params.filter("  ( ( WH_GuiaRemision.FacturaNumero = :factura_numero ) and          ", documentoBean.getDocu_tipodocumento() + documentoBean.getDocu_numero())); //'FC001-0043683' 
        sql.append( params.filter("    ( WH_GuiaRemisionDetalle.ItemCodigo = :cod_producto ) and          ", codProducto)); //'TM0204194'
        sql.append( params.filter("    ( WH_GuiaRemision.CompaniaSocio = :clie_numero ) ", documentoBean.getClie_numero()));//'00000200' 
        sql.append( "  ) ");
        sql.append( " ");
        
		List<OtrosDetalles> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(OtrosDetalles.class));

		return searchResults;
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
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		
		sql.append("SELECT ");
        sql.append(" FROM cabecera");
        sql.append("WHERE  docu_proce_status in ('B','P','E','X') and docu_proce_fecha <=  DATE_SUB(NOW(), INTERVAL 10 MINUTE)");
        sql.append("order by docu_codigo LIMIT 1 ");
		
        
		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));
		
		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}
		
		return null;
	}

}

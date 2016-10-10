package pe.joedayz.etl.dispatcher.impl;

import java.sql.SQLException;
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
import org.springframework.transaction.annotation.Transactional;

import pe.joedayz.etl.beans.DocumentoBean;
import pe.joedayz.etl.beans.OtrosDetalles;
import pe.joedayz.etl.dispatcher.DElectronicoDespachador;
import pe.joedayz.etl.support.WhereParams;

@Repository
public class DElectronicoDespachadorImpl implements DElectronicoDespachador {

	private static final Logger LOGGER = LoggerFactory.getLogger(DElectronicoDespachadorImpl.class);

	@Autowired
	@Qualifier("mysqlDataSource")
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
		sql.append(" EMPR_RAZONSOCIAL,");
		sql.append(" EMPR_UBIGEO,");
		sql.append(" EMPR_NOMBRECOMERCIAL,");
		sql.append(" EMPR_DIRECCION,");
		sql.append(" EMPR_PROVINCIA,");
		sql.append(" EMPR_DEPARTAMENTO,");
		sql.append(" EMPR_DISTRITO,");
		sql.append(" EMPR_PAIS,");
		sql.append(" EMPR_NRORUC,");
		sql.append(" EMPR_TIPODOC,");
		sql.append(" CLIE_NUMERO,");
		sql.append(" CLIE_TIPODOC,");
		sql.append(" CLIE_NOMBRE,");
		sql.append(" DOCU_FECHA,");
		sql.append(" DOCU_TIPODOCUMENTO,");
		sql.append(" DOCU_NUMERO,");
		sql.append(" DOCU_MONEDA,");
		sql.append(" DOCU_GRAVADA  as  DOCU_GRAVADA,");
		sql.append(" DOCU_INAFECTA  as  DOCU_INAFECTA,");
		sql.append(" DOCU_EXONERADA  as  DOCU_EXONERADA,");
		sql.append(" DOCU_GRATUITA  as  DOCU_GRATUITA,");
		sql.append(" DOCU_DESCUENTO  as  DOCU_DESCUENTO,");
		sql.append(" DOCU_SUBTOTAL  as  DOCU_SUBTOTAL,");
		sql.append(" DOCU_TOTAL  as  DOCU_TOTAL,");
		sql.append(" DOCU_IGV  as  DOCU_IGV,");
		sql.append(" TASA_IGV,");
		sql.append(" DOCU_ISC,");
		sql.append(" TASA_ISC,");
		sql.append(" DOCU_OTROSTRIBUTOS  as  DOCU_OTROSTRIBUTOS,");
		sql.append(" TASA_OTROSTRIBUTOS,");

		sql.append(" RETE_REGI,");// 01 TASA 3%
		sql.append(" RETE_TASA,"); // 3%
		sql.append(" RETE_TOTAL_ELEC,"); //
		sql.append(" RETE_TOTAL_RETE,"); //

		sql.append(" DOCU_OTROSCARGOS  as  DOCU_OTROSCARGOS,");
		sql.append(" DOCU_PERCEPCION  as  DOCU_PERCEPCION,");
		sql.append(" NOTA_MOTIVO,");
		sql.append(" NOTA_SUSTENTO,");
		sql.append(" NOTA_TIPODOC,");
		sql.append(" NOTA_DOCUMENTO, ");
		sql.append(" docu_enviaws");
		sql.append(" FROM cabecera");
		sql.append(params.filter(" WHERE  DOCU_CODIGO = :pdocu_codigo", pdocu_codigo));

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}

		return null;
	}

	@Override
	public List<DocumentoBean> cargarDetDocElectronico(String pdocu_codigo) throws SQLException {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  DOCU_CODIGO,");
		sql.append(" DOCU_MONEDA,");
		sql.append(" ITEM_MONEDA,");
		sql.append(" ITEM_ORDEN,");
		sql.append(" ITEM_UNIDAD,");
		sql.append(" ITEM_CANTIDAD,");
		sql.append(" ITEM_CODPRODUCTO,");
		sql.append(" ITEM_DESCRIPCION,");
		sql.append(" ITEM_AFECTACION,");
		sql.append(" ITEM_PVENTA as  ITEM_PVENTA, ");
		sql.append(" ITEM_TI_SUBTOTAL as  ITEM_TI_SUBTOTAL,");
		sql.append(" ITEM_TI_IGV as  ITEM_TI_IGV, ");
		// retenciones
		sql.append(" rete_rela_tipo_docu, ");
		sql.append(" rete_rela_nume_docu, ");
		sql.append(" rete_rela_fech_docu, ");
		sql.append(" rete_rela_tipo_moneda, ");
		sql.append(" rete_rela_total_original, ");
		sql.append(" rete_rela_fecha_pago, ");
		sql.append(" rete_rela_numero_pago, ");
		sql.append(" rete_rela_importe_pagado_original, ");
		sql.append(" rete_rela_tipo_moneda_pago, ");
		sql.append(" rete_importe_retenido_nacional, ");
		sql.append(" rete_importe_neto_nacional, ");
		sql.append(" rete_tipo_moneda_referencia ");
		// Retenciones
		sql.append(" FROM detalle");
		sql.append(params.filter(" WHERE  DOCU_CODIGO = :pdocu_codigo", pdocu_codigo));

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		return searchResults;
	}

	@Override
	public boolean yaExisteDocElectronico(DocumentoBean documentoBean) {
		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		sql.append(" FROM cabecera ");
		sql.append(params.filter(" WHERE DOCU_TIPODOCUMENTO  = :docu_tipodocumento ",
				documentoBean.getDocu_tipodocumento()));
		sql.append(params.filter(" AND   DOCU_NUMERO = :docu_numero ", documentoBean.getDocu_numero()));

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return true;
		}

		return false;
	}

	@Override
	public DocumentoBean noPendienteDocElectronico() {

		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		sql.append(" FROM cabecera");
		sql.append(
				" WHERE  docu_proce_status in ('B','P','E','X') and docu_proce_fecha <=  DATE_SUB(NOW(), INTERVAL 10 MINUTE)");
		sql.append(" order by docu_codigo LIMIT 1 ");

		List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
				new BeanPropertyRowMapper<>(DocumentoBean.class));

		if (searchResults.size() > 0) {
			return searchResults.get(0);
		}

		return null;
	}

	@Override
	@Transactional
	public DocumentoBean insertarDocElectronico(DocumentoBean documentoBean, List<DocumentoBean> plItems) {

		WhereParams params = new WhereParams();
		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO cabecera(");
		sql.append(" EMPR_RAZONSOCIAL,");
		sql.append(" EMPR_UBIGEO,");
		sql.append(" EMPR_NOMBRECOMERCIAL,");
		sql.append(" EMPR_DIRECCION,");
		sql.append(" EMPR_PROVINCIA,");
		sql.append(" EMPR_DEPARTAMENTO,");
		sql.append(" EMPR_DISTRITO,");
		sql.append(" EMPR_PAIS,");
		sql.append(" EMPR_NRORUC,");
		sql.append(" EMPR_TIPODOC,");
		sql.append(" CLIE_NUMERO,");
		sql.append(" CLIE_TIPODOC,");
		sql.append(" CLIE_NOMBRE,");
		sql.append(" DOCU_FECHA,");
		sql.append(" DOCU_TIPODOCUMENTO,");
		sql.append(" DOCU_NUMERO,");
		sql.append(" DOCU_MONEDA,");
		sql.append(" DOCU_GRAVADA,");
		sql.append(" DOCU_INAFECTA,");
		sql.append(" DOCU_EXONERADA,");
		sql.append(" DOCU_GRATUITA,");
		sql.append(" DOCU_DESCUENTO,");
		sql.append(" DOCU_SUBTOTAL,");
		sql.append(" DOCU_TOTAL,");
		sql.append(" DOCU_IGV,");
		sql.append(" TASA_IGV,");
		sql.append(" DOCU_ISC,");
		sql.append(" TASA_ISC,");
		sql.append(" DOCU_OTROSTRIBUTOS,");
		sql.append(" TASA_OTROSTRIBUTOS,");
		sql.append(" DOCU_OTROSCARGOS,");
		sql.append(" DOCU_PERCEPCION,");
		sql.append(" NOTA_MOTIVO,");
		sql.append(" NOTA_SUSTENTO,");
		sql.append(" NOTA_TIPODOC,");
		sql.append(" NOTA_DOCUMENTO, ");
		sql.append(" RETE_REGI,"); // 01 TASA 3%
		sql.append(" RETE_TASA,"); // 3%
		sql.append(" RETE_TOTAL_ELEC,"); //
		sql.append(" RETE_TOTAL_RETE,"); //
		sql.append(" docu_enviaws,");
		sql.append(" docu_proce_status, ");
		sql.append(" docu_forma_pago, ");
		sql.append(" docu_observacion, ");
		sql.append(" clie_direccion, ");
		sql.append(" docu_vendedor, ");
		sql.append(" docu_pedido, ");
		sql.append(" docu_guia_remision, ");
		sql.append(" clie_orden_compra, idExterno) ");
		// 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
		// 6 7 8 9 0 1 2 3 4 5 6 7 8 9
		sql.append(" values (");
		sql.append(params.filter(" :docu_codigo ", documentoBean.getDocu_codigo()));
		sql.append(",");
		sql.append(params.filter(" :docu_moneda ", documentoBean.getDocu_moneda()));
		sql.append(",");
		sql.append(params.filter(" :item_moneda ", documentoBean.getItem_moneda()));
		sql.append(",");
		sql.append(params.filter(" :item_orden ", documentoBean.getItem_orden()));
		sql.append(",");
		sql.append(params.filter(" :item_unidad ", documentoBean.getItem_unidad()));
		sql.append(",");
		sql.append(params.filter(" :item_cantidad ", documentoBean.getItem_cantidad()));
		sql.append(",");
		sql.append(params.filter(" :item_codproducto ", documentoBean.getItem_codproducto()));
		sql.append(",");
		sql.append(params.filter(" :item_descripcion ", documentoBean.getItem_descripcion()));
		sql.append(",");
		sql.append(params.filter(" :item_afectacion ", documentoBean.getItem_afectacion()));
		sql.append(",");
		sql.append(params.filter(" :item_pventa ", documentoBean.getItem_pventa()));
		sql.append(",");
		sql.append(params.filter(" :item_ti_subtotal ", documentoBean.getItem_ti_subtotal()));
		sql.append(",");
		sql.append(params.filter(" :item_ti_igv ", documentoBean.getItem_ti_igv()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_tipo_docu ", documentoBean.getRete_rela_tipo_docu()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_nume_docu ", documentoBean.getRete_rela_nume_docu()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_fech_docu ", documentoBean.getRete_rela_fech_docu()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_tipo_moneda ", documentoBean.getRete_rela_tipo_moneda()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_total_original ", documentoBean.getRete_rela_total_original()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_fecha_pago ", documentoBean.getRete_rela_fecha_pago()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_numero_pago ", documentoBean.getRete_rela_numero_pago()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_importe_pagado_original ",
				documentoBean.getRete_rela_importe_pagado_original()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_tipo_moneda_pago ", documentoBean.getRete_rela_tipo_moneda_pago()));
		sql.append(",");
		sql.append(params.filter(" :rete_rela_tipo_moneda_pago ", documentoBean.getRete_importe_retenido_nacional()));
		sql.append(",");
		sql.append(params.filter(" :rete_importe_neto_nacional ", documentoBean.getRete_importe_neto_nacional()));
		sql.append(",");
		sql.append(params.filter(" :rete_tipo_moneda_referencia ", documentoBean.getRete_tipo_moneda_referencia()));
		sql.append(",");
		sql.append(params.filter(" :item_otros ", documentoBean.getItem_otros()));
		sql.append(")");

		int rowsUpdate = jdbcTemplate.update(sql.toString(), params.getParams());

		if (documentoBean.getOtrosDetalles() != null) {
			if (documentoBean.getOtrosDetalles().size() > 0) {

				params = new WhereParams();
				sql = new StringBuilder();
				sql.append("SELECT * ");
				sql.append(" FROM detalle ");
				sql.append(params.filter(" WHERE docu_codigo = :docu_codigo ", documentoBean.getDocu_codigo()));
				sql.append(params.filter(" AND   item_orden  = ? ", documentoBean.getItem_orden()));

				List<DocumentoBean> searchResults = jdbcTemplate.query(sql.toString(), params.getParams(),
						new BeanPropertyRowMapper<>(DocumentoBean.class));

				if (searchResults.size() > 0) {
					documentoBean.setIddetalle(searchResults.get(0).getIddetalle());
				}
				for (OtrosDetalles otrosDetalle : documentoBean.getOtrosDetalles()) {
					params = new WhereParams();
					sql = new StringBuilder();
					sql.append("INSERT INTO detalle_otro(");
					sql.append(" iddetalle,");
					sql.append(" motor,");
					sql.append(" chasis,");
					sql.append(" color,");
					sql.append(" fabricacion,");
					sql.append(" marca,");
					sql.append(" modelo_periodo,");
					sql.append(" modelo) ");
					sql.append(" VALUES (");
					sql.append(params.filter(" :id_detalle ", documentoBean.getIddetalle()));
					sql.append(",");
					sql.append(params.filter(" :motor ", otrosDetalle.getMotor()));
					sql.append(",");
					sql.append(params.filter(" :chasis ", otrosDetalle.getChasis()));
					sql.append(",");
					sql.append(params.filter(" :color ", otrosDetalle.getColor()));
					sql.append(",");
					sql.append(params.filter(" :fabricacion ", otrosDetalle.getFabricacion()));
					sql.append(",");
					sql.append(params.filter(" :getMarca ", otrosDetalle.getMarca()));
					sql.append(",");
					sql.append(params.filter(" :modelo_periodo ", otrosDetalle.getModelo_periodo()));
					sql.append(",");
					sql.append(params.filter(" :modelo ", otrosDetalle.getModelo()));
					sql.append(")");

					rowsUpdate = jdbcTemplate.update(sql.toString(), params.getParams());
				}

				// cambiamos el estado a nuevo
				params = new WhereParams();
				sql = new StringBuilder();
				sql.append(params.filter("update cabecera set docu_proce_status='N' where docu_codigo= :docu_codigo",
						documentoBean.getDocu_codigo()));
				rowsUpdate = jdbcTemplate.update(sql.toString(), params.getParams());
			}
		}

		return documentoBean;
	}

}

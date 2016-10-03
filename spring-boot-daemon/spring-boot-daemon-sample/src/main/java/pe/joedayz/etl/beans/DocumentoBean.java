/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pe.joedayz.etl.beans;

import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 *
 * @author oswaldo
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
public class DocumentoBean {

    private String docu_codigo;
    private String empr_razonsocial;
    private String empr_ubigeo;
    private String empr_nombrecomercial;
    private String empr_direccion;
    private String empr_provincia;
    private String empr_departamento;
    private String empr_distrito;
    private String empr_pais;
    private String empr_nroruc;
    private String empr_tipodoc;
    private String clie_numero;
    private String clie_tipodoc;
    private String clie_nombre;
    private String docu_fecha;
    private String docu_tipodocumento;
    private String docu_numero;
    private String docu_moneda;
    private String docu_gravada;
    private String docu_inafecta;
    private String docu_exonerada;
    private String docu_gratuita;
    private String docu_descuento;
    private String docu_subtotal;
    private String docu_total;
    private String docu_igv;
    private String tasa_igv;
    private String docu_isc;
    private String tasa_isc;

    private String docu_otrostributos;
    private String docu_otroscargos;
    private String docu_percepcion;
    private String docu_enviaws;
    /**
     * *variable de detalle*****
     */
    private String iddetalle;
    private String item_moneda;
    private String item_orden;
    private String item_unidad;
    private String item_cantidad;
    private String item_codproducto;
    private String item_descripcion;
    private String item_afectacion;
    private String item_pventa;
    private String item_ti_subtotal;
    private String item_ti_igv;
    /**
     * *variable de detalle retenciones*****
     */
    private String rete_rela_tipo_docu;
    private String rete_rela_nume_docu;
    private String rete_rela_fech_docu;
    private String rete_rela_tipo_moneda;
    private String rete_rela_total_original;
    private String rete_rela_fecha_pago;
    private String rete_rela_numero_pago;
    private String rete_rela_importe_pagado_original;
    private String rete_rela_tipo_moneda_pago;
    private String rete_importe_retenido_nacional;
    private String rete_importe_neto_nacional;
    private String rete_tipo_moneda_referencia;
    private String rete_tipo_moneda_objetivo;
    private String rete_tipo_moneda_tipo_cambio;
    private String rete_tipo_moneda_fecha;
    // adicionales de detalle 
    private String item_otros;
    /**
     * variable de resumen**
     */
    private String resu_fecha;
    private String resu_fila;
    private String resu_tipodoc;
    private String resu_serie;
    private String resu_inicio;
    private String resu_final;
    private String resu_gravada;
    private String resu_exonerada;
    private String resu_inafecta;
    private String resu_otcargos;
    private String resu_isc;
    private String resu_igv;
    private String resu_ottributos;
    private String resu_total;
    private String resu_identificador;
    private String resu_fechagenera;
    private String resu_fec;
    private String resu_numero;
    private String resu_motivo;

    //nuevos campos
    private String tasa_otrostributos;
    private String nota_motivo;
    private String nota_sustento;
    private String nota_tipodoc;
    private String nota_documento;
    private String cdesu_nroticket;

    // Retenciones cabecera
    private String rete_regi;
    private String rete_tasa;
    private String rete_total_elec;
    private String rete_total_rete;

    // Adicionales cabecera // facturas
    private String docu_forma_pago;
    private String docu_observacion;
    private String clie_direccion;
    private String docu_vendedor;
    private String docu_pedido;
    private String docu_guia_remision;
    private String clie_orden_compra;

    // Otros detalle es un arreglo
    List<OtrosDetalles> otrosDetalles;

 

}

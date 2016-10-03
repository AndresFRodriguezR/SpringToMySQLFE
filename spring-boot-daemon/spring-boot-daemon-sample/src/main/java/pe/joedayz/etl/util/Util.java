package pe.joedayz.etl.util;

import java.io.FileInputStream;
import java.util.Properties;

public class Util {

    static String eq[][] = { //equivalencia
        {"1", "01", "Factura"},
        {"2", "03", "Boleta"},
        {"RS", "", "Resumen"},
        {"AN", "", "Anulacion"},
        {"6", "07", "Nota de Credito"},
        {"7", "08", "Nota de Debito"},
        {"OE", "08", "Obtener Estado"},
        {"20", "20", "Retenciones"}
    };
    
    static String ruta  = "C:\\home\\certificado\\1\\sunat\\sunat-docs\\config\\";

    public static String getErrorMesageByCode(String errorCode) {
        String msg = "";
        try {
            Properties propiedades = new Properties();
            propiedades.load(new FileInputStream(ruta+"errorFile.properties"));
            msg = errorCode + "|" + propiedades.getProperty(errorCode);
        } catch (Exception ex) {
            msg = "0100|Error en getErrorMesageByCode: " + ex.getMessage();
        }
        return msg;
    }

    public static String getPathZipFilesEnvio() {
        String msg = "";
        try {
            Properties propiedades = new Properties();
            propiedades.load(new FileInputStream(ruta+"property.properties"));
            
            msg = "" + propiedades.getProperty("pathFilesEnvio");
            
        } catch (Exception ex) {
            msg = "Error en getPathFilesEnvio: " + ex.getMessage();
        }
        return msg;
    }

    public static String getPathZipFilesRecepcion() {
        String msg = "";
        try {
            Properties propiedades = new Properties();
            propiedades.load(new FileInputStream(ruta+"property.properties"));
            msg = "" + propiedades.getProperty("pathFilesRecepcion");
        } catch (Exception ex) {
            msg = "Error en getpathFilesRecepcion: " + ex.getMessage();
        }
        return msg;
    }

    public static String getPropertyValue(String paramName) {
        String msg = "";
        try {
            Properties propiedades = new Properties();
            propiedades.load(new FileInputStream(ruta+"property.properties"));

            msg = "" + propiedades.getProperty(paramName);
        } catch (Exception ex) {
            msg = "Error en getPathFiles: " + ex.getMessage();
        }
        return msg;
    }

    public static String equivalenciaTipo(String tipo) {
        String result = "";
        for (int i = 0; i < eq.length; i++) {
            if (eq[i][0].equals(tipo)) {
                result = eq[i][1];
            }
        }
        return result;
    }

    public static String equivalenciaTipoDocNombre(String tipo) {
        String result = "";
        for (int i = 0; i < eq.length; i++) {
            if (eq[i][0].equals(tipo)) {
                result = eq[i][2];
            }
        }
        return result;
    }

}

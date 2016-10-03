package pe.joedayz.etl.beans;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class OtrosDetalles {

    private String iddetalle_otro;
    private String iddetalle;
    private String motor;
    private String chasis;
    private String color;
    private String fabricacion;
    private String marca;
    private String modelo_periodo;
    private String modelo;

}

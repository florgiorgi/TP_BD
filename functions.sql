CREATE OR REPLACE FUNCTION conversion_a_timestamp_fecha_hora_retiro(fecha_hora_retiro TEXT)
RETURNS TIMESTAMP 
AS $$
DECLARE
        rtaTS TIMESTAMP;
BEGIN
        rtaTS = to_timestamp(fecha_hora_retiro, 'DD/MM/YYYY HH24:MI:SS');
		RETURN rtaTS;
END;
$$ LANGUAGE plpgSQL
RETURNS NULL ON NULL INPUT;


CREATE OR REPLACE FUNCTION conversion_a_timestamp_fecha_hora_devolucion(tiempo_uso TEXT, fecha_hora_retiro TIMESTAMP)
RETURNS TIMESTAMP 
AS $$

DECLARE
        rtaTS TIMESTAMP;
        tiempo_uso_time TIME;
        intervalo_auxiliar INTERVAL;
BEGIN
        
		tiempo_uso_time = to_timestamp(tiempo_uso, 'HH24:MI:SS');
		intervalo_auxiliar = tiempo_uso_time + (tiempo_uso_time-tiempo_uso_time);	
        rtaTS = fecha_hora_retiro + intervalo_auxiliar;
       
		RETURN rtaTS;
END;
$$ LANGUAGE plpgSQL
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION tiempo_uso_al_formato_correcto(tiempo_uso TEXT)
RETURNS TEXT AS $$
DECLARE
		rtaTXT TEXT;

        hora TEXT;
        minu TEXT;
        seg TEXT;
        
        position_h INTEGER;
        position_m INTEGER;
        position_seg INTEGER;

BEGIN
        position_h = position('H' in tiempo_uso); 
        position_m = position('M' in tiempo_uso); 
        position_seg = position('S' in tiempo_uso);
       
        hora = substring(tiempo_uso from 1 for position_h-1 ); 
        minu = substring(tiempo_uso from position_h + 2 for  position_m - (position_h + 2) );
        seg = substring(tiempo_uso from position_m + 4 for position_seg - (position_m + 4)); 
    
        rtaTXT = concat_ws(':', hora, minu, seg);
        
        RETURN rtaTXT;
END;
$$ LANGUAGE PLPGSQL
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION conversion_de_tipos_devolucion(fecha_hora_retiro TEXT, tiempo_uso TEXT)
RETURNS TIMESTAMP AS $$
DECLARE
		rtaTS TIMESTAMP;
		fecha_hora_retiro_ts TIMESTAMP;
        tiempo_uso_text_formato_ok TEXT;
BEGIN
		fecha_hora_retiro_ts = conversion_de_tipos_retiro(fecha_hora_retiro);
        tiempo_uso_text_formato_ok = tiempo_uso_al_formato_correcto(tiempo_uso);
       	rtaTS = conversion_a_timestamp_fecha_hora_devolucion(tiempo_uso_text_formato_ok, fecha_hora_retiro_ts);
       	RETURN rtaTS;
END;
$$ LANGUAGE PLPGSQL
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION conversion_de_tipos_retiro(fecha_hora_retiro TEXT)
RETURNS TIMESTAMP AS $$
DECLARE
		rtaTS TIMESTAMP;
BEGIN
        rtaTS = conversion_a_timestamp_fecha_hora_retiro(fecha_hora_retiro);
        RETURN rtaTS;
END;
$$ LANGUAGE PLPGSQL
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION firstRestriction() RETURNS Trigger
AS $$
BEGIN
    IF new.usuario IS NULL THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                            ,new.est_destino,new.fecha_hora_dev;
        raise notice 'El campo usuario era NULL';
        return NULL;
    ELSEIF new.fecha_hora_ret IS NULL THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                      ,new.est_destino,new.fecha_hora_dev;
        raise notice 'El campo fecha_hora_ret era NULL';
        return NULL;
    ELSEIF new.est_origen IS NULL THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                      ,new.est_destino,new.fecha_hora_dev;
        raise notice 'El campo est_origen era NULL';
        return NULL;
    ELSEIF new.est_destino IS NULL THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                      ,new.est_destino,new.fecha_hora_dev;
        raise notice 'El campo est_destino era NULL';
        return NULL;
    ELSEIF new.fecha_hora_dev IS NULL THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                      ,new.est_destino,new.fecha_hora_dev;
        raise notice 'El campo fecha_hora_dev era NULL';
        return NULL;
    ELSE
        return new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER checkFirstRestriction
 BEFORE INSERT ON recorrido_final
 FOR EACH ROW
 EXECUTE PROCEDURE firstRestriction();


CREATE OR REPLACE FUNCTION migracion()
RETURNS VOID AS $$
DECLARE         
        fecha_h_r TIMESTAMP;
        fecha_h_d TIMESTAMP;   
BEGIN  
	    INSERT INTO recorrido_final
	    SELECT periodo, id_usuario, conversion_de_tipos_retiro(fecha_hora_retiro), 
	    	origen_estacion, destino_estacion, conversion_de_tipos_devolucion(fecha_hora_retiro, tiempo_uso)
	    FROM datos_recorrido;
	  
END;
$$ LANGUAGE PLPGSQL;



DO $$
BEGIN
  PERFORM migracion();
END;
$$

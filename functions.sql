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

CREATE OR REPLACE FUNCTION crear_fecha_hora_devolucion(tiempo_uso TIME, fecha_hora_retiro TIMESTAMP)
RETURNS TIMESTAMP 
AS $$

DECLARE
        rtaTS TIMESTAMP;
        intervalo_auxiliar INTERVAL;
BEGIN
        

		intervalo_auxiliar = tiempo_uso + (tiempo_uso-tiempo_uso);	
        rtaTS = fecha_hora_retiro + intervalo_auxiliar;
       
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

--Funcion isNULL--
--Parametros: @field representa un campo de la tupla a revisar, @information representa el texto para el mensaje de error--
--Retorno: True si el campo es null, false sino--
--Uso: Se encarga de chequear que el campo @field no sea null--

CREATE OR REPLACE FUNCTION isNULL(field anyelement, information TEXT) 
RETURNS boolean
AS $$
BEGIN
    IF field IS NULL THEN
        raise notice 'El campo % es NULL', information;
        return true;
    ELSE
        return false;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Funcion isCorrectTime--
--Parametros: @field representa al campo tiempo_uso, @information representa el texto para el mensaje de error--
--Retorno: True si el campo esta mal, false sino--
--Uso: Se encarga de chequear que el campo @field sea un tipo de datos TIME que--
--     represente un intervalo de tiempo mayor a cero--

CREATE OR REPLACE FUNCTION isCorrectTime(field anyelement, information TEXT) 
RETURNS boolean
AS $$
DECLARE
    hours INTEGER;
    minutes INTEGER;
    seconds INTEGER;
    total_seconds INTEGER;
    timeString TEXT = to_char(field, 'HH:MM:SS');
BEGIN
    IF field IS NULL THEN
        raise notice 'El campo % es NULL', information;
        return true;
    END IF;

    SELECT (EXTRACT( HOUR FROM  field::time) * 60*60) INTO hours; 
    SELECT (EXTRACT (MINUTES FROM field::time) * 60) INTO minutes;
    SELECT (EXTRACT (SECONDS from field::time)) INTO seconds;
    SELECT (hours + minutes + seconds) INTO total_seconds;
    IF  total_seconds <= 0 THEN
        raise notice 'El campo % es un tiempo erroneo', information;
        return true;
    ELSE
          return false;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Funcion firstRestriction--
--Parametros: ninguno--
--Retorno: Trigger de tipo checkFirstRestriction--
--Uso: Se encarga de chequear que la restriccion uno se cumpla para toda--
--     tupla a agregar a la tabla--

CREATE OR REPLACE FUNCTION firstRestriction() 
RETURNS Trigger
AS $$
DECLARE
    operation boolean = false;
BEGIN
    operation = operation OR isNULL(new.periodo, 'periodo');
    operation = operation OR isNULL(new.usuario, 'usuario');
    operation = operation OR isNULL(new.fecha_hora_ret, 'fecha_hora_ret');
    operation = operation OR isNULL(new.est_origen, 'est_origen');
    operation = operation OR isNULL(new.est_destino, 'est_destino');
    operation = operation OR isCorrectTime(new.tiempo_uso, 'tiempo_uso');
    
    IF operation THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                       ,new.est_destino, new.tiempo_uso; 
        return NULL;
    ELSE
        return new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER checkFirstRestriction
 BEFORE INSERT ON auxi
 FOR EACH ROW
 EXECUTE PROCEDURE firstRestriction();
 
        CREATE OR REPLACE FUNCTION LIMPIA_REPETIDOS() 
        RETURNS VOID AS $$
        
        DECLARE 
            REP RECORD;
            cursor1 CURSOR FOR SELECT DISTINCT usuario, fecha_hora_ret FROM auxi;            
        
        begin
            open cursor1;
            LOOP
                FETCH cursor1 INTO REP;
                EXIT WHEN NOT FOUND;
                PERFORM GUARDA(REP.usuario, REP.fecha_hora_ret);
            END LOOP;
            CLOSE cursor1;
        end;
        
        $$ LANGUAGE PLPGSQL;
        
        
        
        CREATE OR REPLACE FUNCTION GUARDA
        (myid auxi.usuario%TYPE, my_time auxi.fecha_hora_ret%type) RETURNS VOID AS $$
        
        
        DECLARE
            mycursor CURSOR FOR
            SELECT * FROM auxi
            WHERE myid = usuario AND my_time = fecha_hora_ret
            ORDER BY tiempo_uso ASC;
            CANT INT;
            devolucion TIMESTAMP;
            mystruct RECORD;
            mystruct2 RECORD;
            
        
        BEGIN
        
         OPEN mycursor;
         CANT = 0;
                FETCH mycursor INTO mystruct;
                        FETCH mycursor INTO mystruct2;
                        
                                IF mystruct2.usuario = mystruct.usuario AND mystruct2.fecha_hora_ret = mystruct.fecha_hora_ret THEN
                                         devolucion = crear_fecha_hora_devolucion(mystruct2.tiempo_uso, mystruct2.fecha_hora_ret);
                                         INSERT INTO RECORRIDO_FINAL VALUES(mystruct2.periodo, mystruct2.usuario, mystruct2.fecha_hora_ret, mystruct2.est_origen, mystruct2.est_origen, devolucion);
                                
                                ELSE
                                        devolucion = crear_fecha_hora_devolucion(mystruct.tiempo_uso, mystruct.fecha_hora_ret);
                                        INSERT INTO RECORRIDO_FINAL VALUES(mystruct.periodo, mystruct.usuario, mystruct.fecha_hora_ret, mystruct.est_origen, mystruct.est_origen, devolucion);
                                
                                END IF;

                
         CLOSE mycursor;   
                
       END;
       $$ LANGUAGE PLPGSQL; 
 


CREATE OR REPLACE FUNCTION cond1()
RETURNS VOID AS $$

DECLARE         
        fecha_h_r TIMESTAMP;
        fecha_h_d TIMESTAMP;   
BEGIN  
	    INSERT INTO auxi
	    SELECT DISTINCT periodo, id_usuario, conversion_de_tipos_retiro(fecha_hora_retiro), 
	    	origen_estacion, destino_estacion, to_timestamp(tiempo_uso_al_formato_correcto(tiempo_uso), 'HH24:MI:SS')
	    FROM datos_recorrido;
	  
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION cond2()
RETURNS VOID AS $$
BEGIN  
	    PERFORM LIMPIA_REPETIDOS();	  
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migracion()
RETURNS VOID AS $$

BEGIN
  PERFORM cond1();
  PERFORM cond2();
  
END;
$$ LANGUAGE PLPGSQL;


DO $$
BEGIN
  PERFORM migracion();
END;
$$

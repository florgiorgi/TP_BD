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
        
        posicion_h INTEGER;
        posicion_m INTEGER;
        posicion_seg INTEGER;

BEGIN
        posicion_h = posicion('H' IN tiempo_uso); 
        posicion_m = posicion('M' IN tiempo_uso); 
        posicion_seg = posicion('S' IN tiempo_uso);
       
        hora = substring(tiempo_uso FROM 1 FOR posicion_h-1 ); 
        minu = substring(tiempo_uso FROM posicion_h + 2 FOR  posicion_m - (posicion_h + 2) );
        seg = substring(tiempo_uso FROM posicion_m + 4 FOR posicion_seg - (posicion_m + 4)); 
    
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

--Funcion esNULL--
--Parametros: @field representa un campo de la tupla a revisar, @information representa el texto para el mensaje de error--
--Retorno: True si el campo es null, false sino--
--Uso: Se encarga de chequear que el campo @field no sea null--

CREATE OR REPLACE FUNCTION esNULL(field un_elemento, informacion TEXT) 
RETURNS boolean
AS $$
BEGIN
    IF field IS NULL THEN
        raise notice 'El campo % es NULL', informacion;
        RETURN true;
    ELSE
        RETURN false;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Funcion esCorrectoTime--
--Parametros: @field representa al campo tiempo_uso, @information representa el texto para el mensaje de error--
--Retorno: True si el campo esta mal, false sino--
--Uso: Se encarga de chequear que el campo @field sea un tipo de datos TIME que--
--     represente un intervalo de tiempo mayor a cero--

CREATE OR REPLACE FUNCTION esCorrectoTime(field un_elemento, informacion TEXT) 
RETURNS boolean
AS $$
DECLARE
    horas INTEGER;
    minutos INTEGER;
    segundos INTEGER;
    total_segundos INTEGER;
    timeString TEXT = to_char(field, 'HH:MM:SS');
BEGIN
    IF field IS NULL THEN
        RAISE NOTICE 'El campo % es NULL', informacion;
        RETURN true;
    END IF;

    SELECT (EXTRACT( HOUR FROM  field::time) * 60*60) INTO horas; 
    SELECT (EXTRACT (MINUTES FROM field::time) * 60) INTO minutos;
    SELECT (EXTRACT (SECONDS FROM field::time)) INTO segundos;
    SELECT (horas + minutos + segundos) INTO total_segundos;
    IF  total_segundos <= 0 THEN
        RAISE NOTICE 'El campo % es un tiempo erroneo', informacion;
        RETURN true;
    ELSE
          RETURN false;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Funcion primeraRestriction--
--Parametros: ninguno--
--Retorno: Trigger de tipo chequearPrimeraRestriccion--
--Uso: Se encarga de chequear que la restriccion uno se cumpla para toda--
--     tupla a agregar a la tabla--

CREATE OR REPLACE FUNCTION primeraRestriccion() 
RETURNS Trigger
AS $$
DECLARE
    operacion BOOLEAN = false;
BEGIN
    operacion = operacion OR esNULL(new.periodo, 'periodo');
    operacion = operacion OR esNULL(new.usuario, 'usuario');
    operacion = operacion OR esNULL(new.fecha_hora_ret, 'fecha_hora_ret');
    operacion = operacion OR esNULL(new.est_origen, 'est_origen');
    operacion = operacion OR esNULL(new.est_destino, 'est_destino');
    operacion = operacion OR esCorrectoTime(new.tiempo_uso, 'tiempo_uso');
    
    IF operation THEN
        RAISE NOTICE 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                       ,new.est_destino, new.tiempo_uso; 
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER chequearPrimeraRestriccion
BEFORE INSERT ON auxi
FOR EACH ROW
EXECUTE PROCEDURE primeraRestriccion();
 
 
 
 
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
            cant INT;
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
       
       
       
       
 --Funcion problemaSolapados--
--Parametros: @usuario_id es el usuario al que vamos a buscarle problemas de solapamiento--
--Retorno: retorna 1 si ejecuto sin problemas --
--Uso: Se encarga de juntar tuplas solapadas e insertarlas en recorrido_final--  

 CREATE OR REPLACE FUNCTION problemaSolapados(usuario_id INTEGER) RETURNS INTEGER
AS $$

DECLARE

	cursorSolap CURSOR FOR
	SELECT * FROM auxi
	WHERE usuario = usuario_id
	ORDER BY fecha_hora_ret ASC;
	structSolap auxi;
	
	origen INTEGER;
	destino INTEGER;
	periodo TEXT;
    
    fechaRetiro TIMESTAMP;
	fechaDevolucion TIMESTAMP;


BEGIN

		OPEN cursorSolap;
		FETCH cursorSolap INTO structSolap;

		LOOP

			EXIT WHEN structSolap ISNULL;
            
            origen = structSolap.est_origen;
            destino = structSolap.est_destino;
			periodo = structSolap.periodo;
            
			fechaRetiro = structSolap.fecha_hora_ret;
			fechaDevolucion = structSolap.fecha_hora_dev;
			
			

			LOOP

                FETCH cursorSolap INTO structSolap;
                EXIT WHEN NOT FOUND OR structSolap.fecha_hora_ret > fechaDevolucion;
                fechaDevolucion = structSolap.fecha_hora_dev;
                destino = structSolap.est_destino;

			END LOOP;

			INSERT INTO recorrido_final VALUES(periodo, usuario_id, fechaRetiro, origen, destino, fechaDevolucion);

		END LOOP;
		CLOSE cursorSolap;
        
RETURN 1;
END;
$$ LANGUAGE plpgSQL;


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

CREATE OR REPLACE FUNCTION cond3() RETURNS VOID
AS $$
DECLARE

		id INTEGER;
 		cursor3 CURSOR FOR
		SELECT DISTINCT usuario FROM auxi2;

BEGIN
		OPEN cursor3;
		LOOP

			FETCH cursor3 INTO id;
			EXIT WHEN NOT FOUND;
			PERFORM problemaSolapados(id);

		END LOOP;
		CLOSE cursor3;

		RETURN 1;
END;
$$ LANGUAGE plpgSQL;



CREATE OR REPLACE FUNCTION triggerSolap()
RETURNS VOID AS $$

BEGIN
	CREATE TRIGGER detecta_solapado BEFORE INSERT ON RECORRIDO_FINAL
	FOR EACH ROW
	EXECUTE PROCEDURE detecta_solapado();
END;

$$ LANGUAGE plpgsql;


--Funcion detecta_solapado--
--Parametros: ninguno--
--Retorno: Trigger --
--Uso: Comprueba que no se viole condicion de solapamiento en una nueva insercion--
CREATE OR REPLACE FUNCTION detecta_solapado()
RETURNS Trigger AS $$

DECLARE
 	cant INT;
    
BEGIN
	SELECT count(*) INTO cant
	FROM RECORRIDO_FINAL
	WHERE usuario = new.usuario AND (fecha_hora_ret <= new.fecha_hora_dev) AND (fecha_hora_dev >= new.fecha_hora_ret);

	IF (cant > 0) THEN
    
		RAISE EXCEPTION 'Error: Elementos solapados';
        
	END IF;

	RETURN new;
END;

$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION migracion()
RETURNS VOID AS $$

BEGIN
  PERFORM cond1();
  PERFORM cond2();
  /* condicion 3 */
  PERFORM triggerSolap();
  /* DROP TABLE datos_recorrido */
  /* DROP TABLE auxi */
  /* DROP TABLE auxi2 */
  
END;
$$ LANGUAGE PLPGSQL;


DO $$
BEGIN
  PERFORM migracion();
END;
$$

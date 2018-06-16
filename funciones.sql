-- Importación --

-- Guia para importar a una tabla auxiliar los datos que estan en los csv --

-- 1) Abrir DBVisualizer
-- 2) Descargar los archivos csv de Campus
-- 3) Mandar los archivos csv descargados a pampero. En Windows usar WinSCP. En Linux ejecutar el comando: 
-- scp 'path-archivo-localhost' usuario@pampero.itba.edu.ar:path-archivo-remoto 
-- Ejemplo: scp '/home/rocio/Descargas/test2.csv' rdella@pampero.itba.edu.ar:/home/rdella
-- 4) Conectarse a la base de datos de pampero: ssh usuario@pampero.itba.edu.ar -L 5432:bd1.it.itba.edu.ar:5432
-- 5) Una vez en la terminal de pampero, ejecutar psql -h bd1.it.itba.edu.ar -U usuario PROOF
-- 6) En SQL Commander (DBVisualizer) y correrlo:

SET datestyle = "ISO, DMY";

CREATE TABLE datos_recorrido
(periodo                TEXT,
id_usuario              INTEGER,
fecha_hora_retiro       TEXT,
origen_estacion         INTEGER,
nombre_origen           TEXT,
destino_estacion        INTEGER,
nombre_destino          TEXT,
tiempo_uso              TEXT,
fecha_creacion          TEXT
);

CREATE TABLE auxi
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
tiempo_uso TIME,
PRIMARY KEY(periodo, usuario,fecha_hora_ret,est_origen, est_destino, tiempo_uso));

CREATE TABLE auxi2
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >=
fecha_hora_ret),
PRIMARY KEY(periodo, usuario,fecha_hora_ret, est_origen, est_destino, fecha_hora_dev));

CREATE TABLE recorrido_final
(periodo TEXT,
usuario INTEGER,
fecha_hora_ret TIMESTAMP NOT NULL,
est_origen INTEGER NOT NULL,
est_destino INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >=
fecha_hora_ret),
PRIMARY KEY(usuario,fecha_hora_ret));

-- 7) En la terminal que tenemos abierta: PROOF =>

SET datestyle = dmy; 
\copy datos_recorrido FROM 'path-archivo-remoto' header delimiter ';' csv;





--Funcion conversion_a_timestamp_fecha_hora_retiro--
--Parametros: @fecha_hora_retiro--
--Retorno: La fecha ingresada en TIMESTAMP, NULL si el parametro es NULL--
--Uso: Se encarga de convertir un TEXT a TIMESTAMP--

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


--Funcion crear_fecha_hora_devolucion--
--Parametros: @tiempo_uso, @fecha_hora_retiro--
--Retorno: La fecha creada en TIMESTAMP, NULL si los parametros son NULL--
--Uso: Se encarga de crear un nuevo campo a partir de los valores de @tiempo_uso y @fecha_hora_retiro--

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


--Funcion conversion_a_timestamp_fecha_hora_devolucion--
--Parametros: @tiempo_uso, @fecha_hora_retiro--
--Retorno: La fecha ingresada en TIMESTAMP, NULL si los parametros son NULL--
--Uso: Se encarga de crear un nuevo campo a partir de los valores de @tiempo_uso y @fecha_hora_retiro--

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


--Funcion tiempo_uso_al_formato_correcto--
--Parametros: @tiempo_uso--
--Retorno: El tiempo de uso al formato correcto 00:00:00, NULL si el parametro es NULL--
--Uso: Se encarga de pasar el campo @tiempo_uso al formato correcto para la tabla final--
--      desglosando el TEXT que llega por parametro--

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
    posicion_h = position('H' IN tiempo_uso); 
    posicion_m = position('M' IN tiempo_uso); 
    posicion_seg = position('S' IN tiempo_uso);
       
    hora = substring(tiempo_uso FROM 1 FOR posicion_h-1 ); 
    minu = substring(tiempo_uso FROM posicion_h + 2 FOR  posicion_m - (posicion_h + 2) );
    seg = substring(tiempo_uso FROM posicion_m + 4 FOR posicion_seg - (posicion_m + 4)); 
    
    rtaTXT = concat_ws(':', hora, minu, seg);
    RETURN rtaTXT;

END;
$$ LANGUAGE plpgSQL
RETURNS NULL ON NULL INPUT;


--Funcion conversion_de_tipos_devolucion--
--Parametros: @fecha_hora_retiro, @tiempo_uso--
--Retorno: La fecha convertida en TIMESTAMP--
--Uso: Se encarga de convertir dos TEXT a TIMESTAMP--

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
$$ LANGUAGE plpgSQL
RETURNS NULL ON NULL INPUT;


--Funcion conversion_de_tipos_retiro--
--Parametros: @fecha_hora_retiro--
--Retorno: La fecha convertida en TIMESTAMP--
--Uso: Se encarga de convertir dos TEXT a TIMESTAMP--

CREATE OR REPLACE FUNCTION conversion_de_tipos_retiro(fecha_hora_retiro TEXT)
RETURNS TIMESTAMP AS $$

DECLARE
		rtaTS TIMESTAMP;

BEGIN
        rtaTS = conversion_a_timestamp_fecha_hora_retiro(fecha_hora_retiro);
        RETURN rtaTS;

END;
$$ LANGUAGE plpgSQL
RETURNS NULL ON NULL INPUT;


--Funcion esNULL--
--Parametros: @field representa un campo de la tupla a revisar, @information representa el texto para el mensaje de error--
--Retorno: True si el campo es null, false sino--
--Uso: Se encarga de chequear que el campo @field no sea null--

CREATE OR REPLACE FUNCTION esNULL(field anyelement, informacion TEXT) 
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

CREATE OR REPLACE FUNCTION esCorrectoTime(field anyelement, informacion TEXT) 
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
    
    IF operacion THEN
        RAISE NOTICE 'No se pudo insertar % % % % % %',new.periodo, new.usuario, new.fecha_hora_ret,new.est_origen 
                                                       ,new.est_destino, new.tiempo_uso; 
        RETURN NULL;
    ELSE
        RETURN new;
    END IF;

END;
$$ LANGUAGE plpgsql;

--TRIGGER--
CREATE TRIGGER chequearPrimeraRestriccion
BEFORE INSERT ON auxi
FOR EACH ROW
EXECUTE PROCEDURE primeraRestriccion();


--Funcion limpia_repetidos--
--Parametros: --
--Retorno: --
--Uso: Se encarga agrupar aquellas tuplas en auxi que tengan identico id y fecha de retiro--
--  con el fin de pasarle los datos a segundo para que la inserte en la tabla aux2--  
CREATE OR REPLACE FUNCTION limpia_repetidos() 
RETURNS VOID AS $$
        
DECLARE 
    REP RECORD;
    devolucion TIMESTAMP;
        
    cursor1 CURSOR FOR 
    SELECT DISTINCT usuario, fecha_hora_ret FROM auxi
    GROUP BY usuario, fecha_hora_ret
    HAVING count(usuario) > 1;  
            
BEGIN
    OPEN cursor1;
    LOOP
        FETCH cursor1 INTO REP;
        EXIT WHEN NOT FOUND;
        PERFORM segundo(REP.usuario, REP.fecha_hora_ret);
        END LOOP;
    CLOSE cursor1;  

END;    
$$ LANGUAGE PLPGSQL;

        
--Funcion agrego_faltantes--
--Parametros: @myid, @my_time --
--Retorno: --
--Uso: Se encarga agregar a aux2 las tuplas que corresponden--
--  con los segundos de aquellos que tienen identico id y fecha de retiro--  
CREATE OR REPLACE FUNCTION segundo(myid auxi.usuario%TYPE, my_time auxi.fecha_hora_ret%type) 
RETURNS VOID AS $$
               
DECLARE
    pointer auxi;
    firstFetch auxi;
    devolucion TIMESTAMP;
    cursor2 CURSOR FOR
    SELECT * FROM auxi
    WHERE usuario = myid and fecha_hora_ret = my_time
    ORDER BY tiempo_uso ASC;            
        
BEGIN
    OPEN cursor2;

    FETCH cursor2 INTO firstFetch;
    FETCH cursor2 INTO pointer;

    CLOSE cursor2;

    devolucion = crear_fecha_hora_devolucion(pointer.tiempo_uso, pointer.fecha_hora_ret);
    INSERT INTO auxi2 VALUES(pointer.periodo, pointer.usuario, pointer.fecha_hora_ret, pointer.est_origen, pointer.est_destino, devolucion);

END;
$$ LANGUAGE PLPGSQL;
       
       
--Funcion agrego_faltantes--
--Parametros: --
--Retorno: --
--Uso: Se encarga agregar a aux2 las tuplas que faltan, es decir
--    que no fueron tenidas en cuenta en la funcion segundo--  

CREATE OR REPLACE FUNCTION agrego_faltantes() 
RETURNS VOID AS $$
        
DECLARE

BEGIN
    INSERT INTO auxi2
    SELECT periodo, usuario, fecha_hora_ret, est_origen, est_destino, crear_fecha_hora_devolucion(tiempo_uso, fecha_hora_ret)
    FROM auxi
    WHERE (usuario, fecha_hora_ret) NOT IN (SELECT usuario, fecha_hora_ret FROM auxi2);

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION agrego_faltantes2() 
        RETURNS VOID AS $$
        
        DECLARE

        
        BEGIN
                INSERT INTO recorrido_final
                SELECT periodo, usuario, fecha_hora_ret, est_origen, est_destino, fecha_hora_dev
                FROM auxi2
                WHERE usuario NOT IN (SELECT usuario FROM recorrido_final);
          END;
       $$ LANGUAGE PLPGSQL;

DO $$
BEGIN
  PERFORM migracion();
END;
$$
       

--Funcion problemaSolapados--
--Parametros: @usuario_id es el usuario al que vamos a buscarle problemas de solapamiento--
--Retorno: --
--Uso: Se encarga de juntar tuplas solapadas e insertarlas en recorrido_final--  

CREATE OR REPLACE FUNCTION problemaSolapados(usuario_id INTEGER) RETURNS VOID
AS $$

DECLARE

    cursorSolap CURSOR FOR
    SELECT * FROM auxi2
    WHERE usuario = usuario_id
    ORDER BY fecha_hora_ret ASC;
    structSolap auxi2;
    
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
    PERFORM limpia_repetidos();
    PERFORM agrego_faltantes(); 

END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION cond3() RETURNS VOID
AS $$
DECLARE

        id INTEGER;
        cursor3 CURSOR FOR
        SELECT DISTINCT usuario FROM auxi2
        GROUP BY usuario
        HAVING COUNT(*) > 1;

BEGIN
        OPEN cursor3;
        LOOP

            FETCH cursor3 INTO id;
            EXIT WHEN NOT FOUND;
            PERFORM problemaSolapados(id);

        END LOOP;
        CLOSE cursor3;

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
--Parametros: --
--Retorno: Trigger --
--Uso: Comprueba que no se viole condicion de solapamiento en una nueva insercion--
CREATE OR REPLACE FUNCTION detecta_solapado()
RETURNS Trigger AS $$

DECLARE
 	cant INT;
    
BEGIN
	SELECT count(*) INTO cant
	FROM recorrido_final
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
  PERFORM cond3();
  PERFORM agrego_faltantes2();
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

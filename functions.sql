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

CREATE OR REPLACE FUNCTION isNULL(field anyelement, information TEXT) RETURNS boolean
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

CREATE OR REPLACE FUNCTION isLessThanZero(tiempo_uso TEXT, information TEXT) RETURNS boolean
AS $$
DECLARE
    hours INTEGER;
    minutes INTEGER;
    seconds INTEGER;
    total_seconds INTEGER;
    tiempo_uso_correcto TEXT;
    tiempo_uso_time TIME;
    
BEGIN
    tiempo_uso_correcto = tiempo_uso_al_formato_correcto(tiempo_uso);
    tiempo_uso_time = to_timestamp(tiempo_uso_correcto, 'HH24:MI:SS');
    IF tiempo_uso IS NULL THEN
        return true;
    END IF;
    SELECT (EXTRACT( HOUR FROM  tiempo_uso_time) * 60*60) INTO hours; 
    SELECT (EXTRACT (MINUTES FROM tiempo_uso_time) * 60) INTO minutes;
    SELECT (EXTRACT (SECONDS FROM tiempo_uso_time)) INTO seconds;
    SELECT (hours + minutes + seconds) INTO total_seconds;
    IF  total_seconds <= 0 THEN
        raise notice 'El campo % es un tiempo erroneo', information;
        return true;
    ELSE
          return false;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION firstRestriction() RETURNS Trigger
AS $$
DECLARE
    operation boolean = false;
BEGIN
    RAISE NOTICE 'HERE %',operation;
    operation = operation OR isNULL(new.periodo, 'periodo');
    operation = operation OR isNULL(new.id_usuario, 'id_usuario');
    operation = operation OR isNULL(new.fecha_hora_retiro, 'fecha_hora_retiro');
    operation = operation OR isNULL(new.origen_estacion, 'origen_estacion');
    operation = operation OR isNULL(new.destino_estacion, 'destino_estacion');
    operation = operation OR isNULL(new.tiempo_uso, 'tiempo_uso');
    operation = operation OR isLessThanZero(new.tiempo_uso, 'tiempo_uso');
    
    IF operation THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.id_usuario, new.fecha_hora_retiro,new.origen_estacion 
                                                       ,new.destino_estacion, new.tiempo_uso; 
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
 
 /*Inserta en auxi, la cual tiene un trigger asociado para evitar ingreso de campos prohibidos en NULL */
CREATE OR REPLACE FUNCTION cond1()
RETURNS VOID AS $$

BEGIN  
	    INSERT INTO auxi
	    SELECT *
	    FROM datos_recorrido;
	  
END;
$$ LANGUAGE PLPGSQL;
 
/*Unifica tuplas que tienen igual id_usuario y fecha_hora_retiro, seleccionando en caso de que haya dos o mas, el segundo en orden
por tiempo de uso*/
CREATE OR REPLACE FUNCTION cond2()
RETURNS VOID AS $$
  
DECLARE

    customStruct RECORD;
    myRec RECORD;
    
BEGIN

    FOR customStruct IN select * from auxi LOOP
    CREATE TABLE matches AS SELECT * FROM auxi WHERE
    customStruct.id_usuario || customStruct.fecha_hora_retiro = auxi.id_usuario || auxi.fecha_hora_retiro
    ORDER BY CAST(replace(replace(replace(auxi.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL);


    IF (select count(*) from matches)>1 then
    
      FOR myRec IN SELECT * FROM matches LOOP
      END LOOP;
      
      FOR myRec IN SELECT * FROM matches LIMIT 1 LOOP
        DELETE FROM auxi WHERE (myRec.periodo = auxi.periodo AND myRec.origen_estacion = auxi.origen_estacion AND
        myRec.nombre_origen = auxi.nombre_origen AND myRec.destino_estacion = auxi.destino_estacion AND
        myRec.nombre_destino = auxi.nombre_destino AND myRec.tiempo_uso=auxi.tiempo_uso AND myRec.fecha_creacion=auxi.fecha_creacion);
      END LOOP;

      FOR match IN SELECT * FROM matches OFFSET 2 LOOP
        DELETE FROM auxi WHERE (myRec.periodo = auxi.periodo AND myRec.origen_estacion = auxi.origen_estacion AND
        myRec.nombre_origen = auxi.nombre_origen AND myRec.destino_estacion = auxi.destino_estacion AND
        myRec.nombre_destino = auxi.nombre_destino AND myRec.tiempo_uso=auxi.tiempo_uso AND myRec.fecha_creacion=auxi.fecha_creacion);
      END LOOP;
      
      
    END IF;
    
    
    
    DROP TABLE matches;
  END LOOP;
  
END;
$$ LANGUAGE PLPGSQL;

/*Hace los casteos correspondientes para insertar en recorrido_final */
CREATE OR REPLACE FUNCTION cond3()
RETURNS VOID AS $$
DECLARE
	cond3struct RECORD;
    mycursor CURSOR FOR SELECT * FROM auxi;
BEGIN
    OPEN mycursor;
        LOOP
            FETCH cursor INTO cond3struct;
            EXIT WHEN NOT FOUND;
            
            PERFORM finalInsert(cond3struct.periodo, CAST(cond3struct.id_usuario AS INTEGER), CAST(cond3struct.fecha_hora_retiro AS TIMESTAMP), CAST(cond3struct.origen_estacion AS INTEGER), CAST(cond3struct.destino_estacion AS INTEGER), CAST(cond3struct.fecha_hora_retiro AS TIMESTAMP) + CAST(replace(replace(replace(cond3struct.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL));
            
 	    END LOOP;
 	CLOSE mycursor;
END;
$$ LANGUAGE PLPGSQL;

/*Inserta en recorrido final, aplicando la tercera condicion*/
CREATE OR REPLACE FUNCTION finalInsert(periodo TEXT, usuario INTEGER, fecha_hora_ret TIMESTAMP, est_origen INTEGER, est_destino INTEGER, fecha_hora_dev TIMESTAMP)
RETURNS VOID AS $$

DECLARE
    
    pfecha_hora_ret TIMESTAMP = fecha_hora_ret;
    pest_origen INTEGER = est_origen;
    pest_destino INTEGER = est_destino;
    pfecha_hora_dev TIMESTAMP = fecha_hora_dev;
    mystruct RECORD;
BEGIN

  FOR mystruct IN SELECT * FROM recorrido_final LOOP
    IF usuario = mystruct.usuario AND (fecha_hora_ret <= mystruct.fecha_hora_dev) AND (fecha_hora_dev >= mystruct.fecha_hora_ret) THEN

      CASE
      
        WHEN fecha_hora_ret<=mystruct.fecha_hora_ret THEN
          UPDATE recorrido_final set fecha_hora_ret = pfecha_hora_ret, est_origen = pest_origen WHERE recorrido_final=mystruct;
          
     ELSE

          UPDATE recorrido_final set est_destino = pest_destino, fecha_hora_dev = pfecha_hora_dev WHERE recorrido_final=mystruct;
      END CASE;
    RETURN;
    END IF;
  END LOOP;

  INSERT INTO recorrido_final VALUES(periodo, usuario, fecha_hora_ret, est_origen, est_destino, fecha_hora_dev);
  EXCEPTION
	WHEN OTHERS THEN
		raise notice '% %', SQLSTATE, SQLERRM;
    END;
$$ LANGUAGE PLPGSQL;

/*Funcion para crear el trigger detecta_solapados*/
CREATE OR REPLACE FUNCTION triggerSolap()
RETURNS VOID AS $$

BEGIN
	CREATE TRIGGER detecta_solapado BEFORE INSERT ON RECORRIDO_FINAL
	FOR EACH ROW
	EXECUTE PROCEDURE detecta_solapado();
END;

$$ LANGUAGE plpgsql;


/*Trigger detecta_solapado */
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
  PERFORM cond3();
  PERFORM triggerSolap();
END;
$$ LANGUAGE PLPGSQL;


DO $$
BEGIN
  PERFORM migracion();
END;
$$

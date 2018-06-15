

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
    tiempo_uso_time TIME;
BEGIN
    tiempo_uso_time = to_timestamp(tiempo_uso, 'HH24:MI:SS');
    IF field IS NULL THEN
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
    operation = operation OR isNULL(new.periodo, 'periodo');
    operation = operation OR isNULL(new.id_usuario, 'id_usuario');
    operation = operation OR isNULL(new.fecha_hora_retiro, 'fecha_hora_retiro');
    operation = operation OR isNULL(new.origen_estacion, 'origen_estacion');
    operation = operation OR isNULL(new.destino_estacion, 'destino_estacion');
    operation = operation OR isNULL(new.tiempo_uso, 'tiempo_uso');
    /*operation = operation OR isLessThanZero(new.tiempo_uso, 'tiempo_uso');*/
    
    IF operation THEN
        raise notice 'No se pudo insertar % % % % % %',new.periodo, new.id_usuario, new.fecha_hora_retiro,new.origen_estacion 
                                                       ,new.destino_estacion, new.tiempo_uso; 
        return NULL;
    ELSE
     raise notice 'INSERTANDO ',new.periodo, new.id_usuario, new.fecha_hora_retiro,new.origen_estacion 
                                                       ,new.destino_estacion, new.tiempo_uso; 
        return new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER checkFirstRestriction
 BEFORE INSERT ON auxi
 FOR EACH ROW
 EXECUTE PROCEDURE firstRestriction();
 
CREATE OR REPLACE FUNCTION cond1()
RETURNS VOID AS $$

BEGIN  
	    INSERT INTO auxi
	    SELECT *
	    FROM datos_recorrido;
	  
END;
$$ LANGUAGE PLPGSQL;
 

CREATE OR REPLACE FUNCTION cond2()
RETURNS VOID AS $$
  
DECLARE
	cursor CURSOR FOR SELECT * FROM auxi;
    tuple RECORD;
    match RECORD;
    aux1 RECORD;
BEGIN
    FOR tuple IN select * from auxi LOOP
    CREATE TABLE matches AS SELECT * FROM auxi WHERE
    tuple.id_usuario || tuple.fecha_hora_retiro = auxi.id_usuario || auxi.fecha_hora_retiro
    ORDER BY CAST(replace(replace(replace(auxi.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL);

    IF (select count(*) from matches)>1 then
      FOR match IN SELECT * FROM matches LOOP
      END LOOP;
      FOR match IN SELECT * FROM matches LIMIT 1 LOOP
        DELETE FROM auxi WHERE (match.periodo = auxi.periodo AND match.origen_estacion = auxi.origen_estacion AND
        match.nombre_origen = auxi.nombre_origen AND match.destino_estacion = auxi.destino_estacion AND
        match.nombre_destino = auxi.nombre_destino AND match.tiempo_uso=auxi.tiempo_uso AND match.fecha_creacion=auxi.fecha_creacion);
      END LOOP;

      FOR match IN SELECT * FROM matches OFFSET 2 LOOP
        DELETE FROM auxi WHERE (match.periodo = auxi.periodo AND match.origen_estacion = auxi.origen_estacion AND
        match.nombre_origen = auxi.nombre_origen AND match.destino_estacion = auxi.destino_estacion AND
        match.nombre_destino = auxi.nombre_destino AND match.tiempo_uso=auxi.tiempo_uso AND match.fecha_creacion=auxi.fecha_creacion);
      END LOOP;
    END IF;

    DROP TABLE matches;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION cond3()
RETURNS VOID AS $$
DECLARE
	aRec RECORD;
    cursor CURSOR FOR SELECT * FROM auxi;
BEGIN
    OPEN cursor;
        LOOP
            FETCH cursor INTO aRec;
            EXIT WHEN NOT FOUND;
            PERFORM insertar_recorrido(aRec.periodo, CAST(aRec.id_usuario AS INTEGER), CAST(aRec.fecha_hora_retiro AS TIMESTAMP), CAST(aRec.origen_estacion AS INTEGER), CAST(aRec.destino_estacion AS INTEGER), CAST(aRec.fecha_hora_retiro AS TIMESTAMP) + CAST(replace(replace(replace(aRec.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL));
 	    END LOOP;
 	CLOSE cursor;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION insertar_recorrido(periodo TEXT, usuario INTEGER, fecha_hora_ret TIMESTAMP, est_origen INTEGER, est_destino INTEGER, fecha_hora_dev TIMESTAMP)
RETURNS VOID AS $$
#variable_conflict use_column
DECLARE
tuple RECORD;
pfecha_hora_ret TIMESTAMP = fecha_hora_ret;
pest_origen INTEGER = est_origen;
pest_destino INTEGER = est_destino;
pfecha_hora_dev TIMESTAMP = fecha_hora_dev;
BEGIN
  ---------------------------
  -- solapados encadenados --
  ---------------------------
  FOR tuple IN SELECT * FROM recorrido_final LOOP
    IF usuario = tuple.usuario AND (fecha_hora_ret <= tuple.fecha_hora_dev) AND (fecha_hora_dev >= tuple.fecha_hora_ret) THEN

      -- tupla dada solapa menor a tupla de tabla --
      CASE
        WHEN fecha_hora_ret<=tuple.fecha_hora_ret THEN
          UPDATE recorrido_final set fecha_hora_ret = pfecha_hora_ret, est_origen = pest_origen WHERE recorrido_final=tuple;
        ELSE
      -- tupla de tabla solapa menor a tupla dada --
          UPDATE recorrido_final set est_destino = pest_destino, fecha_hora_dev = pfecha_hora_dev WHERE recorrido_final=tuple;
      END CASE;
    RETURN;
    END IF;
  END LOOP;
  ---------------------------

  INSERT INTO recorrido_final VALUES(periodo, usuario, fecha_hora_ret, est_origen, est_destino, fecha_hora_dev);
  EXCEPTION
	WHEN OTHERS THEN
		raise notice '% %', SQLSTATE, SQLERRM;
    END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION triggerSolap()
RETURNS VOID AS $$
BEGIN
	CREATE TRIGGER detecta_solapado BEFORE INSERT ON RECORRIDO_FINAL
	FOR EACH ROW
	EXECUTE PROCEDURE detecta_solapado();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION detecta_solapado()
RETURNS Trigger AS $$
DECLARE
 	countSolapados INT;
BEGIN
	SELECT count(*) INTO countSolapados
	FROM RECORRIDO_FINAL
	WHERE usuario = new.usuario AND (fecha_hora_ret <= new.fecha_hora_dev) AND (fecha_hora_dev >= new.fecha_hora_ret);

	IF (countSolapados > 0) THEN
		RAISE EXCEPTION 'INSERCION IMPOSIBLE POR SOLAPAMIENTO';
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

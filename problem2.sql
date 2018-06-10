CREATE OR REPLACE FUNCTION LIMPIA_REPETIDOS() RETURNS VOID 
AS $$

DECLARE 
    cursor1 CURSOR FOR SELECT DISTINCT id, fecha_hora FROM auxi ORDER BY tiempo_uso DESC;
    

REP RECORD;
begin
    open cursor1;
    LOOP
        FETCH cursor1 INTO REP;
        EXIT WHEN NOT FOUND;
        PERFORM GUARDA(REP.id, REP.fecha_hora);
    END LOOP;
end;

$$LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION GUARDA
(myid auxi.id%TYPE, my_time auxi.fecha_hora%type) RETURNS VOID AS $$
DECLARE
    mycursor CURSOR FOR
    SELECT * FROM auxi
    WHERE myid = id AND my_time = fecha_hora
    ORDER BY tiempo_uso;
    
REP RECORD;
CANT INT;

BEGIN
 OPEN mycursor;
 FETCH mycursor INTO REP;
 CANT = 0;

LOOP
 FETCH mycursor INTO REP;
 EXIT WHEN NOT FOUND;
 IF CANT == 2 THEN
 INSERT INTO RECORRIDO_FINAL VALUES(/*todas las columnas ?? */);
 ELSE
 CANT = CANT + 1;
 END IF;
 END LOOP;
 IF CANT == 1 THEN
 INSERT INTO RECORRIDO_FINAL VALUES(/*todas las columnas ?? */);
 CLOSE mycursor;
END;
$$ LANGUAGE PLPGSQL;



SELECT LIMPIA_REPETIDOS(); /* esto llama a la funcion cuando se corre todo*/
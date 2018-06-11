        CREATE OR REPLACE FUNCTION LIMPIA_REPETIDOS() 
        RETURNS VOID AS $$
        
        DECLARE 
            REP RECORD;
            cursor1 CURSOR FOR SELECT DISTINCT id_usuario, fecha_hora_retiro, tiempo_uso FROM datos_recorrido ORDER BY tiempo_uso DESC;            
        
        begin
            open cursor1;
            LOOP
                FETCH cursor1 INTO REP;
                EXIT WHEN NOT FOUND;
                PERFORM GUARDA(REP.id_usuario, REP.fecha_hora_retiro);
            END LOOP;
            CLOSE cursor1;
        end;
        
        $$ LANGUAGE PLPGSQL;
        
        
        
        CREATE OR REPLACE FUNCTION GUARDA
        (myid datos_recorrido.id_usuario%TYPE, my_time datos_recorrido.fecha_hora_retiro%type) RETURNS VOID AS $$
        DECLARE
            mycursor CURSOR FOR
            SELECT * FROM datos_recorrido
            WHERE myid = id_usuario AND my_time = fecha_hora_retiro
            ORDER BY tiempo_uso;
            
        REP RECORD;
        CANT INT;
        
        BEGIN
         OPEN mycursor;
         FETCH mycursor INTO REP;
         CANT = 1;
        
                LOOP
                         FETCH mycursor INTO REP;
                         EXIT WHEN NOT FOUND;
                         IF CANT = 2 THEN 
                                 INSERT INTO RECORRIDO_FINAL VALUES('201608','74710','2016-08-13 13:03:00','28','528','2016-08-13 13:34:00');
                                 ELSE
                                 CANT = CANT + 1;
                         END IF;
                END LOOP;
                IF CANT = 1 THEN
                         INSERT INTO RECORRIDO_FINAL VALUES('201608','74710','2016-08-13 13:03:00','28','528','2016-08-13 13:34:00');
                END IF;
                CLOSE mycursor;
                END;
        $$ LANGUAGE PLPGSQL;
        
        SELECT LIMPIA_REPETIDOS();

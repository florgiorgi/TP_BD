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
        
        
        DO $$
        BEGIN
          PERFORM LIMPIA_REPETIDOS();
        END;
        $$
        




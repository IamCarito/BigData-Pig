senalizaciones = LOAD 'hdfs://quickstart.cloudera:8020/user/cloudera/memento/senalizacion.csv'
USING PigStorage('|')
AS
(gis_x:chararray,
gis_y:chararray,
fecha_alta:chararray,
fecha_baja:chararray,
distrito:chararray,
barrio:chararray,
calle:chararray,
finca:chararray,
tipo_senalizacion:chararray,
codigo:chararray,
descripcion);
 
senales_vigentes = FILTER senalizaciones BY ((fecha_baja matches '.*2020') or (fecha_baja matches '.*9999'));

/*
--Cuenta cuantas tuplas hay en señales vigentes
grupo_senales_vig = GROUP senales_vigentes ALL;
cuenta_senales_vig = foreach grupo_senales_vig Generate COUNT(senales_vigentes);
dump cuenta_senales_vig;
*/


senales_por_verificar = FILTER senales_vigentes BY NOT(
(fecha_alta matches '.*2013') or
(fecha_alta matches '.*2014') or
(fecha_alta matches '.*2015') or
(fecha_alta matches '.*2016') or 
(fecha_alta matches '.*2017') or
(fecha_alta matches '.*2018') or
(fecha_alta matches '.*2019') or
(fecha_alta matches '.*2020') );

senales_en_norma = FILTER senales_vigentes BY (
(fecha_alta matches '.*2013') or
(fecha_alta matches '.*2014') or
(fecha_alta matches '.*2015') or
(fecha_alta matches '.*2016') or
(fecha_alta matches '.*2017') or
(fecha_alta matches '.*2018') or
(fecha_alta matches '.*2019') or
(fecha_alta matches '.*2020') );


senales_orientacion = FILTER senales_vigentes BY (tipo_senalizacion == 'Carteles de Orientación');

senales_codigo = FILTER senales_vigentes BY (tipo_senalizacion == 'Señales de Código');
codigos_p = FILTER senales_codigo BY (codigo matches '.*P-.*');
codigos_r = FILTER senales_codigo BY (codigo matches '.*R-.*');
codigos_s = FILTER senales_codigo BY (codigo matches '.*S-.*');
codigos_ts = FILTER senales_codigo BY (codigo matches '.*TS-.*');


codigo_otros = FILTER senales_codigo BY NOT
((codigo matches '.*P-.*') or (codigo matches '.*R-.*') or (codigo matches '.*S-.*') or
 (codigo matches '.*TS-.*') or (codigo matches 'Código'));


-- Salida de archivos
-- Almacena el resultado en un archivo en HDFS

STORE senales_vigentes INTO 'senales_vigentes1.csv' USING PigStorage ('|');
STORE senales_por_verificar INTO 'senales_por_verificar.csv' USING PigStorage ('|');
STORE senales_en_norma INTO 'senales_en_norma.csv' USING PigStorage ('|');
STORE senales_orientacion INTO 'senales_orientacion.csv' USING PigStorage ('|');
STORE senales_codigo INTO 'senales_codigo.csv' USING PigStorage ('|');




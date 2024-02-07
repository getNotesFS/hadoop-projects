


 

#Consulta de Electric_Vehicle_Population_Data.csv

-- Cargar Datos
vehicles = LOAD 'hdfs://localhost:9000/input_ec/Electric_Vehicle_Population_Data.csv' USING PigStorage(',') 
   AS (VIN:chararray, County:chararray, City:chararray, State:chararray, 
   Postal_Code:int, Model_Year:int, Make:chararray, Model:chararray, 
   EV_Type:chararray, CAFV_Eligibility:chararray, Electric_Range:int, 
   Base_MSRP:int, Legislative_District:int, DOL_Vehicle_ID:long, 
   Vehicle_Location:chararray, Electric_Utility:chararray, Census_Tract:chararray);

-- Filtrar Datos
filtered_vehicles = FILTER vehicles BY (City == 'Olympia');

-- Agrupar Datos
grouped_vehicles = GROUP filtered_vehicles BY (County, EV_Type);

-- Contar Vehículos por Categoría
vehicle_count = FOREACH grouped_vehicles GENERATE FLATTEN(group) AS (County, EV_Type), COUNT(filtered_vehicles) AS count;

-- Almacenar Resultados
STORE vehicle_count INTO 'hdfs://localhost:9000/output/vehicle_count_by_county_2' USING PigStorage(',');




# Consulta de 4 archivos en PIG hdfs://localhost:9000/navarrainput/input

alojamientos = LOAD 'hdfs://localhost:9000/navarrainput/input2/alojamientos.csv' USING PigStorage(';') AS (
    id: chararray, COD_INSCRIPCION: chararray,
    NOMBRE: chararray, MODALIDAD: chararray, CATEGORIA: chararray, DIRECCION: chararray, 
    LOCALIDAD: chararray, MUNICIPIO: chararray, ZONA: chararray,
    PLAZAS: chararray, FECHA_INSCRIPCION: chararray
);
alojamientos = FOREACH alojamientos GENERATE 
    (int)REPLACE(id, '"', '') AS id, 
    REPLACE(COD_INSCRIPCION, '"', '') AS COD_INSCRIPCION, 
    REPLACE(NOMBRE, '"', '') AS NOMBRE, 
    REPLACE(MODALIDAD, '"', '') AS MODALIDAD, 
    REPLACE(CATEGORIA, '"', '') AS CATEGORIA, 
    REPLACE(DIRECCION, '"', '') AS DIRECCION, 
    TRIM(REPLACE(LOCALIDAD, '"', '')) AS LOCALIDAD, 
    TRIM(REPLACE(MUNICIPIO, '"', '')) AS MUNICIPIO, 
    REPLACE(ZONA, '"', '') AS ZONA, 
    (int)REPLACE(PLAZAS, '"', '') AS PLAZAS, 
    REPLACE(FECHA_INSCRIPCION, '"', '') AS FECHA_INSCRIPCION;


empresas = LOAD 'hdfs://localhost:9000/navarrainput/input2/empresas.csv' USING PigStorage(';') AS (
    id: chararray,
    CIF_NIF_NIE: chararray,
    No_Identificacion_RII: chararray,
    Titular: chararray,
    Denominacion_Rotulo_del_establecimiento: chararray,
    Direccion: chararray,
    Municipio: chararray,
    Localidad: chararray,
    Provincia: chararray,
    CP: chararray,
    Telefono: chararray,
    Email: chararray,
    Fax: chararray,
    URL: chararray,
    Actividad_Principal: chararray,
    Descripcion_Actividad_Principal: chararray,
    Actividades_Secundarias: chararray,
    Descripcion_Actividades_Secundarias: chararray
);
empresas = FOREACH empresas GENERATE 
    (int)REPLACE(id, '"', '') AS id, 
    REPLACE(CIF_NIF_NIE, '"', '') AS CIF_NIF_NIE, 
    REPLACE(No_Identificacion_RII, '"', '') AS No_Identificacion_RII, 
    REPLACE(Titular, '"', '') AS Titular, 
    REPLACE(Denominacion_Rotulo_del_establecimiento, '"', '') AS Denominacion_Rotulo_del_establecimiento, 
    REPLACE(Direccion, '"', '') AS Direccion, 
    TRIM(REPLACE(Municipio, '"', '')) AS Municipio, 
    TRIM(REPLACE(Localidad, '"', '')) AS Localidad, 
    REPLACE(Provincia, '"', '') AS Provincia, 
    REPLACE(CP, '"', '') AS CP, 
    REPLACE(Telefono, '"', '') AS Telefono, 
    REPLACE(Email, '"', '') AS Email, 
    REPLACE(Fax, '"', '') AS Fax, 
    REPLACE(URL, '"', '') AS URL, 
    REPLACE(Actividad_Principal, '"', '') AS Actividad_Principal, 
    REPLACE(Descripcion_Actividad_Principal, '"', '') AS Descripcion_Actividad_Principal, 
    REPLACE(Actividades_Secundarias, '"', '') AS Actividades_Secundarias, 
    REPLACE(Descripcion_Actividades_Secundarias, '"', '') AS Descripcion_Actividades_Secundarias;


restaurantes = LOAD 'hdfs://localhost:9000/navarrainput/input2/restaurantes.csv' USING PigStorage(';') AS (
    id: chararray, COD_INSCRIPCION: chararray, NOMBRE: chararray, MODALIDAD: chararray, 
    CATEGORIA: chararray, DIRECCION: chararray, LOCALIDAD: chararray, MUNICIPIO: chararray, 
    ZONA: chararray, Especialidad: chararray, FECHA_INSCRIPCION: chararray
);
restaurantes = FOREACH restaurantes GENERATE 
    (int)REPLACE(id, '"', '') AS id, 
    REPLACE(COD_INSCRIPCION, '"', '') AS COD_INSCRIPCION, 
    REPLACE(NOMBRE, '"', '') AS NOMBRE, 
    REPLACE(MODALIDAD, '"', '') AS MODALIDAD, 
    REPLACE(CATEGORIA, '"', '') AS CATEGORIA, 
    REPLACE(DIRECCION, '"', '') AS DIRECCION, 
    TRIM(REPLACE(LOCALIDAD, '"', '')) AS LOCALIDAD, 
    TRIM(REPLACE(MUNICIPIO, '"', '')) AS MUNICIPIO, 
    REPLACE(ZONA, '"', '') AS ZONA, 
    REPLACE(Especialidad, '"', '') AS Especialidad, 
    REPLACE(FECHA_INSCRIPCION, '"', '') AS FECHA_INSCRIPCION;


farmacias = LOAD 'hdfs://localhost:9000/navarrainput/input2/farmacias.csv' USING PigStorage(';') AS (
    id: chararray, FECHA: chararray, DESDE: chararray, HASTA: chararray, LOCALIDAD: chararray, 
    GRUPO: chararray, DIRECCION: chararray, FARMACIA: chararray, Cod_FARMACIA: chararray, TELEFONO: chararray
);
farmacias = FOREACH farmacias GENERATE 
    (int)REPLACE(id, '"', '') AS id, 
    REPLACE(FECHA, '"', '') AS FECHA, 
    (int)REPLACE(DESDE, '"', '') AS DESDE, 
    (int)REPLACE(HASTA, '"', '') AS HASTA, 
    TRIM(REPLACE(LOCALIDAD, '"', '')) AS LOCALIDAD, 
    REPLACE(GRUPO, '"', '') AS GRUPO, 
    REPLACE(DIRECCION, '"', '') AS DIRECCION, 
    REPLACE(FARMACIA, '"', '') AS FARMACIA, 
    REPLACE(Cod_FARMACIA, '"', '') AS Cod_FARMACIA, 
    REPLACE(TELEFONO, '"', '') AS TELEFONO;




-- Agrupar alojamientos por localidad y contar
alojamientos_por_localidad = GROUP alojamientos BY LOCALIDAD;
conteo_alojamientos = FOREACH alojamientos_por_localidad GENERATE group AS localidad, COUNT(alojamientos) AS total_alojamientos;

-- Agrupar restaurantes por localidad y contar
restaurantes_por_localidad = GROUP restaurantes BY LOCALIDAD;
conteo_restaurantes = FOREACH restaurantes_por_localidad GENERATE group AS localidad, COUNT(restaurantes) AS total_restaurantes;

-- Agrupar farmacias por localidad y contar
farmacias_por_localidad = GROUP farmacias BY LOCALIDAD;
conteo_farmacias = FOREACH farmacias_por_localidad GENERATE group AS localidad, COUNT(farmacias) AS total_farmacias;

-- Agrupar empresas por localidad y contar
empresas_por_localidad = GROUP empresas BY Localidad;
conteo_empresas = FOREACH empresas_por_localidad GENERATE group AS localidad, COUNT(empresas) AS total_empresas;
 
-- Normalizar los nombres de localidades en conteo_total_aloj_rest
conteo_total_aloj_rest_norm = FOREACH conteo_total_aloj_rest GENERATE 
    UPPER(TRIM(localidad)) AS localidad,
    total_alojamientos,
    total_restaurantes;

-- Normalizar los nombres de localidades en conteo_farmacias
conteo_farmacias_norm = FOREACH conteo_farmacias GENERATE 
    UPPER(TRIM(localidad)) AS localidad,
    total_farmacias;

-- Unir los conteos de alojamientos, restaurantes y farmacias
conteo_total_servicios = JOIN conteo_total_aloj_rest_norm BY localidad, conteo_farmacias_norm BY localidad;
conteo_servicios = FOREACH conteo_total_servicios GENERATE 
    conteo_total_aloj_rest_norm::localidad AS localidad,
    conteo_total_aloj_rest_norm::conteo_alojamientos::total_alojamientos AS total_alojamientos,
    conteo_total_aloj_rest_norm::conteo_restaurantes::total_restaurantes AS total_restaurantes,
    conteo_farmacias_norm::total_farmacias AS total_farmacias;

DUMP conteo_servicios;


-- ESTA PARTE NO FUNCIONA PORQUE NO CONCUERDA EL TIPO DE DATO DE LOCALIDAD ENTRE LOS DATOS DE SERVICIOS Y EMPRESAS
-- Unir los conteos de servicios y empresas para comparar
comparacion_servicios_empresas = JOIN conteo_servicios BY localidad, conteo_empresas BY localidad;
datos_comparativos = FOREACH comparacion_servicios_empresas GENERATE 
    conteo_servicios::localidad AS localidad, 
    conteo_servicios::total_alojamientos AS total_alojamientos,
    conteo_servicios::total_restaurantes AS total_restaurantes,
    conteo_servicios::total_farmacias AS total_farmacias,
    conteo_empresas::total_empresas AS total_empresas;

-- Mostrar los resultados de la comparación
DUMP comparacion_servicios_empresas;

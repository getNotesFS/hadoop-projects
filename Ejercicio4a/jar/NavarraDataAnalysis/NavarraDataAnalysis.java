import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.List;

public class NavarraDataAnalysis {

    public static class EmpresaMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = CSVParser.parseLine(line);

            if (tokens.length > 0) {
                // Asumiendo que la localidad está en una columna 8
                String localidad = tokens[7].trim();

                // Normalización de la localidad para casos como " "
                localidad = UtilidadesMapReduce.normalizarLocalidad(localidad);

                // Crear un Text para la localidad y otro para el resto de la información de la
                // empresa
                Text localidadKey = new Text(localidad);
                Text empresaInfo = new Text("EMPRESA_" + line);
                context.write(localidadKey, empresaInfo);
            }
        }

    }

    public static class FarmaciaMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = CSVParser.parseLine(line);

            if (tokens.length > 0) {
                String localidad = tokens[4].trim(); 

                // Normalización de la localidad
                localidad = UtilidadesMapReduce.normalizarLocalidad(localidad);

                // Crear un Text para la localidad y otro para el resto de la información de la
                // farmacia
                Text localidadKey = new Text(localidad);
                Text farmaciaInfo = new Text("FARMACIA_" + line);
                context.write(localidadKey, farmaciaInfo);
            }
        }

    }

    public static class RestauranteMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = CSVParser.parseLine(line);

            if (tokens.length > 0) {
                String localidad = tokens[6].trim(); 

                // Normalización de la localidad
                localidad = UtilidadesMapReduce.normalizarLocalidad(localidad);

                // Crear un Text para la localidad y otro para el resto de la información del
                // restaurante
                Text localidadKey = new Text(localidad);
                Text restauranteInfo = new Text("RESTAURANTE_" + line);

                context.write(localidadKey, restauranteInfo);
            }
        }
    }

    public static class AlojamientoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = CSVParser.parseLine(line);
            if (tokens.length > 0) {
                // según tu archivo
                String localidad = tokens[6].trim(); 

                // Normalización de la localidad
                localidad = UtilidadesMapReduce.normalizarLocalidad(localidad);

                // Crear un Text para la localidad y otro para el resto de la información del
                // alojamiento
                Text localidadKey = new Text(localidad);
                Text alojamientoInfo = new Text("ALOJAMIENTO_" + line);

                context.write(localidadKey, alojamientoInfo);
            }
        }
    }

    public static class UtilidadesMapReduce {

        public static String normalizarLocalidad(String localidad) { 
            if (localidad.length() == 0) {
                localidad = "UNKNOWN";
            }
            return localidad;
        }
    }

    public static class CSVParser {

        public static String[] parseLine(String line) {
            List<String> tokens = new ArrayList<>();
            boolean inQuotes = false;
            StringBuilder sb = new StringBuilder();

            char[] chars = line.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                char c = chars[i];

                if (c == '\"') {
                    // Si estamos dentro de las comillas, verifica el siguiente carácter
                    if (inQuotes && i < chars.length - 1 && chars[i + 1] == '\"') {
                        sb.append(c); // Añade una comilla doble y salta la siguiente
                        i++; 
                    } else {
                        inQuotes = !inQuotes; // Cambia el estado de estar dentro o fuera de las comillas
                    }
                } else if (c == ';' && !inQuotes) {
                    tokens.add(sb.toString().trim()); // Añade el token al finalizar un campo
                    sb = new StringBuilder(); // Reinicia el StringBuilder para el próximo campo
                } else {
                    sb.append(c); // Añade el carácter al StringBuilder
                }
            }
            tokens.add(sb.toString().trim()); // Añade el último campo

            return tokens.toArray(new String[0]);
        }
    }


    // Reducer
    public static class DataReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int contadorEmpresas = 0;
            int contadorFarmacias = 0;
            int contadorAlojamientos = 0;
            int contadorRestaurantes = 0;

            for (Text value : values) {
                String line = value.toString();

                if (line.startsWith("EMPRESA_")) {
                    contadorEmpresas++;
                } else if (line.startsWith("FARMACIA_")) {
                    contadorFarmacias++;
                } else if (line.startsWith("ALOJAMIENTO_")) {
                    contadorAlojamientos++;
                } else if (line.startsWith("RESTAURANTE_")) {
                    contadorRestaurantes++;
                }
            }

            String salida = String.format("Empresas: %d, Farmacias: %d, Alojamientos: %d, Restaurantes: %d",
                    contadorEmpresas, contadorFarmacias, contadorAlojamientos, contadorRestaurantes);
            context.write(key, new Text(salida));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Navarra Data Analysis");

        job.setJarByClass(NavarraDataAnalysis.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configura MultipleInputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EmpresaMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FarmaciaMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RestauranteMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, AlojamientoMapper.class);

        // Reducir y escribir en el directorio de salida
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        job.setReducerClass(DataReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

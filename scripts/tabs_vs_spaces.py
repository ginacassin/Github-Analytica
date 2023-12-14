from script_interface import ScriptInterface
from pyspark.sql.functions import col  # Import col function
from pyspark.sql import functions as F

class TabsVsSpaces(ScriptInterface):
    def __init__(self):
        super().__init__('Top 5 licenses')

    def tabs_vs_spaces(self, contents_df):

        # Normalize and clean extensions
        filtered_df = (
            contents_df
            .withColumn("ext", F.lower(F.regexp_replace(F.regexp_extract("path", r'\.([^\.]*)$', 1), "[^a-z]", "")))
            .filter("ext != ''")
        )

        # Calculate metrics
        result_df = (
            filtered_df
            .withColumn("tabs", F.when(F.size(F.expr("FILTER(SPLIT(content, '\\n'), x -> regexp_extract(x, '^[ \\t]+', 0) = '\\t')")) > 0, 1).otherwise(0))
            .withColumn("spaces", F.when(F.size(F.expr("FILTER(SPLIT(content, '\\n'), x -> regexp_extract(x, '^[ \\t]+', 0) = ' ')")) > 0, 1).otherwise(0))
            .withColumn("lratio", F.log((F.col("spaces") + 1) / (F.col("tabs") + 1)))
            .withColumn("countext", F.lit(1).alias("countext"))
            .select("ext", "tabs", "spaces", "countext", "lratio")
            .orderBy("countext", ascending=False)
            .limit(100)
        )

        # Add 'predominant' column
        result_df = result_df.withColumn("predominant", F.when(F.col("tabs") > F.col("spaces"), "tab")
            .when(F.col("tabs") < F.col("spaces"), "space")
            .otherwise("balanced"))

        # Group by extension and count files in each category
        grouped_df = result_df.groupBy("ext").agg(
            F.sum(F.when(F.col("predominant") == "tab", 1)).alias("files_w_tabs"),
            F.sum(F.when(F.col("predominant") == "space", 1)).alias("files_w_spaces")
        )

        # Ordenar por la suma total de archivos y seleccionar las 5 principales
        top_5_result = (
            grouped_df
            .withColumn("total_files", F.col("files_w_tabs") + F.col("files_w_spaces"))
            .orderBy(F.col("total_files").desc())
            .limit(70)
        )

        # Save the result to a CSV file
        self.save_data(grouped_df, 'tabs_vs_spaces')

        return top_5_result

    def process_data(self):
        # Obtener 'contents_df' mediante 'get_contents'
        contents_df = self.get_table('contents')

        # Obtener el resultado de 'tabs_vs_spaces'
        top_licenses_df = self.tabs_vs_spaces(contents_df)

        # Log y mostrar el resultado (si está en modo de prueba)
        if self.test_mode:
            top_licenses_df.show(truncate=False)
            self.log.info('Top 5 licenses: \n %s', top_licenses_df.toPandas())

        # Guardar el resultado en un archivo CSV
        self.save_data(top_licenses_df, 'tabs_vs_spaces')

    def get_contents(self):
        # Obtener 'files_df' y 'contents_df'
        files_df = self.get_table('files')
        contents_df = self.get_table('contents')

        # Filtrar los archivos por extensión
        filtered_files_df = files_df.filter(col("path").rlike(r'\.([^\.]*)$'))

        # Agregar el nombre de la carpeta a las rutas
        result_files_df = (
            filtered_files_df
            .groupBy("id")
            .agg(
                F.first("path").alias("path"),
                F.first("repo_name").alias("repo_name")
            )
        )

        # Realizar la unión de los marcos de datos
        result_df = result_files_df.join(
            contents_df,
            result_files_df["id"] == contents_df["id"],
            how="inner"
        ).select(
            result_files_df["id"],
            contents_df["size"],
            contents_df["content"],
            contents_df["binary"],
            contents_df["copies"],
            result_files_df["repo_name"],
            result_files_df["path"]
        )

        return result_df

if __name__ == "__main__":
    # Crear una instancia de la clase y ejecutar el procesamiento
    top_licenses = TabsVsSpaces()
    top_licenses.run()

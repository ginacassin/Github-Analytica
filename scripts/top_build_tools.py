from script_interface import ScriptInterface
from pyspark.sql.functions import col


class TopBuildTools(ScriptInterface):
    def __init__(self):
        super().__init__('TopBuildTools')

    def top_build_tools_used(self):
        """
        Works with 'files' table.
        Obtains the number of repositories that use each of the build tools.
        :return: A DataFrame with the number of repositories that use each of the build tools.
        """

        build_tool_files = [
            "Makefile",
            "CMakeLists.txt",  # Archivo de configuración para CMake
            "pom.xml",        # Archivo de configuración para Maven
            "build.gradle",   # Archivo de configuración para Gradle
            "Dockerfile",
            "Jenkinsfile",    # Configuración de Jenkins en un archivo
            ".travis.yml",    # Configuración de Travis CI en un archivo
            "webpack.config.js",  # Configuración para Webpack
            "package.json",   # Archivo de configuración para npm
            "yarn.lock",      # Archivo de bloqueo de versiones para yarn
            "requirements.txt",  # Archivo de requisitos para pip
            "webpack.config.js",  # Configuración para Webpack
            "build.xml",      # Archivo de configuración para Ant
            "XcodeProj",      # Carpeta de configuración para proyectos Xcode
            "build.sh"        # Script de construcción general
        ]

        # Obtain 'files' table
        files_df = self.get_table('files')

        # Inicializa un diccionario para almacenar el recuento de cada herramienta y la lista de repositorios
        tool_info = {}

        # Calcula el recuento y la lista de repositorios para cada herramienta
        for keyword in build_tool_files:
            filtered_df = files_df.filter(col("path").contains(keyword))
            unique_repos = set(filtered_df.select("repo_name").distinct().rdd.flatMap(lambda x: x).collect())
            tool_info[keyword] = {"count": len(unique_repos), "repositories": list(unique_repos)}

        # Crea un nuevo DataFrame a partir del diccionario
        result_data = [(tool, info["count"]) for tool, info in tool_info.items()]

        # Especifica manualmente los tipos de datos para cada columna
        result_df = self.spark.createDataFrame(result_data, ["tool", "count_repos"],
                                        ["string", "integer"])
        
        result_df = result_df.orderBy(col("count_repos").desc())

        return result_df

    def process_data(self):
        top_build_tools_used = self.top_build_tools_used()

        # Log and print the result (if in test mode)
        if self.test_mode:
            top_build_tools_used.show(truncate=False)
            self.log.info('Build tools used by repo: \n %s', top_build_tools_used.toPandas())

        # Save the result to a CSV file
        self.save_data(top_build_tools_used, 'top_build_tools_used')


if __name__ == "__main__":
    top_build_tools_used = TopBuildTools()
    top_build_tools_used.run()
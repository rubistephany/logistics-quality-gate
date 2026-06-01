# Notas Técnicas y Resolución del Laboratorio LOGIXPRESS

Este documento explica las adaptaciones técnicas realizadas para que el Quality Gate sea funcional en un entorno real con Spark 3.5.1, Great Expectations y Soda Core, superando las limitaciones técnicas del borrador original.

---

## 1. ¿Por qué este proyecto funciona? (Mejoras Técnicas)

Para que el proyecto fuera ejecutable y robusto, se realizaron las siguientes "correcciones de ingeniería":

### A. Gestión de Dependencias Críticas
*   **Problema**: Soda Core estándar no reconoce DataFrames de Spark en memoria por defecto.
*   **Solución**: Se instaló específicamente `soda-core-spark-df` y se añadieron librerías de soporte de red (`pyhive`, `thrift`, `thrift-sasl`). Esto permite que el adaptador de Spark funcione sin errores de "módulo no encontrado".

### B. El "Script Puente" para Soda (`script_soda.py`)
*   **Problema**: Las **Vistas Temporales** de Spark (`createOrReplaceTempView`) solo existen dentro de la sesión de Python que las crea. Ejecutar el comando `soda scan` desde la terminal de Linux falla porque Soda no puede ver la tabla en memoria.
*   **Solución**: Se desarrolló un script de Python que levanta la SparkSession, registra el DataFrame y ejecuta el motor de Soda **dentro del mismo proceso**. Esto garantiza que Soda siempre tenga acceso a los datos.

### C. Evolución de la API de Great Expectations
*   **Problema**: El código sugerido originalmente utilizaba métodos obsoletos de versiones antiguas de GX.
*   **Solución**: Se implementó la **API Fluent** (introducida en la v0.18+) utilizando objetos `Validator` y `Datasources` definidos mediante código, lo que asegura compatibilidad con Spark 3.5.1.

### D. Sintaxis de SodaCL para Spark
*   **Problema**: Ciertos checks como `valid format: regex` o comparaciones directas de fechas con `NOW` fallan en el motor de Spark de Soda.
*   **Solución**: Se ajustó la sintaxis a `valid regex:` y se utilizó la función nativa `freshness()`, que es la forma correcta y optimizada de validar la actualidad de los datos en Soda Core.

---

## 2. Conclusión Final de la Implementación
El Quality Gate desarrollado protege a LOGIXPRESS de datos mal formados, nulos o duplicados. La resolución de estos desafíos técnicos demuestra una comprensión profunda de la arquitectura de datos distribuida y de la integración de herramientas de calidad en un entorno Big Data.
"""Dataset sucio de ejemplo para el Quality Gate de LOGIXPRESS Inc.

La tabla simulada se llama conceptualmente `entregas_silver` y representa
datos que llegan desde las aplicaciones móviles de repartidores a la capa Silver
del Data Lakehouse.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)


def get_spark_session(app_name: str = "LogixpressQuality") -> SparkSession:
    """Crea una SparkSession local compatible con PySpark, GX y Soda."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def get_spark_dataset() -> tuple[SparkSession, DataFrame]:
    """Devuelve la SparkSession y el DataFrame `entregas_silver`.

    El dataset contiene errores intencionales para demostrar el rechazo del
    Quality Gate:
    - id_entrega nulo.
    - id_entrega duplicado.
    - estado inválido.
    - codigo_postal_destino con formato incorrecto.
    - intentos_entrega fuera del rango permitido.
    - una fecha con más de 48 horas de antigüedad.
    """
    spark = get_spark_session()

    data = [
        ("E001", "P100", datetime.now().date(), "ENTREGADO", None, "28001", "Seur", 1),
        ("E002", "P101", datetime.now().date() - timedelta(days=3), "FALLIDO", "Dirección incorrecta", "08002", "MRW", 2),
        (None, "P102", datetime.now().date(), "PENDIENTE", None, "41003", "Correos", 1),
        ("E004", "P103", datetime.now().date(), "INVALIDO", None, "2800", "Seur", 3),
        ("E005", "P104", datetime.now().date(), "DEVUELTO", "Rechazado", "50005", "MRW", 6),
        ("E001", "P105", datetime.now().date(), "ENTREGADO", None, "28001", "Seur", 1),
    ]

    schema = StructType([
        StructField("id_entrega", StringType(), True),
        StructField("id_pedido", StringType(), True),
        StructField("fecha_entrega", DateType(), True),
        StructField("estado", StringType(), True),
        StructField("motivo_fallo", StringType(), True),
        StructField("codigo_postal_destino", StringType(), True),
        StructField("empresa_transporte", StringType(), True),
        StructField("intentos_entrega", IntegerType(), True),
    ])

    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("entregas_silver")
    return spark, df


if __name__ == "__main__":
    spark_session, df_entregas = get_spark_dataset()
    df_entregas.show(truncate=False)
    df_entregas.printSchema()
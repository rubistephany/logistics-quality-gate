
# EJERCICIO DE GX Y SODA

## Para que tus alumnos se metan 100% en el papel de un Data Engineer

Lo mejor es darles un contexto de negocio realista. Aquí tienes el storytelling y el escenario de la empresa ficticia para presentar al inicio de la clase.

# 📦 El Contexto: LOGIXPRESS Inc.

LOGIXPRESS Inc. es una empresa internacional de comercio electrónico y logística de última milla que gestiona la entrega de más de 500.000 paquetes diarios.

Acabas de incorporarte al equipo de Data Engineering. El Director de Datos (CDO) te reúne de urgencia porque el departamento de Atención al Cliente y el equipo de Data Science están colapsando debido a fallos críticos en los reportes matutinos.

# 🚨 El Problema de Negocio

El pipeline actual recoge los datos directamente de las aplicaciones móviles de los repartidores y los vuelca en la capa Silver del Data Lakehouse (entregas_silver). Sin embargo, el sistema no tiene ningún control de calidad (Quality Gate).

Esto ha provocado incidentes graves esta semana:

- Paquetes duplicados que duplican las comisiones de transporte.
- Códigos postales con menos de 5 dígitos que desvían los camiones a provincias incorrectas.
- Pérdida de trazabilidad de pedidos porque el identificador llega vacío (null).
- Reclamaciones masivas de clientes porque los estados del paquete bailan en la base de datos de manera caótica.

# 🎯 Tu Misión como Data Engineer

Para frenar esta sangría de dinero y quejas, tu primera gran tarea es diseñar un Quality Gate estricto a la entrada de la capa Silver.

Vas a auditar una muestra del dataset (entregas_silver) utilizando dos herramientas de mercado para evaluar cuál se adapta mejor al flujo de la empresa:

- Great Expectations (para validación programática en los pipelines de Python).
- Soda Core (para validaciones declarativas rápidas y alertas).

# 📋 Especificaciones del Contrato de Datos (Data Contract)

| Columna | Tipo de Dato | Dimensión DAMA | Regla de Negocio |
|----------|----------|----------|----------|
| id_entrega | STRING | Unicidad y Completitud | Es la Clave Primaria. No puede ser nulo ni estar duplicado. |
| id_pedido | STRING | Completitud | No puede ser nulo (todo envío nace de un pedido). |
| estado | STRING | Validez | Solo se permiten: ENTREGADO, FALLIDO, PENDIENTE, DEVUELTO |
| intentos_entrega | INTEGER | Rango | Debe ser un valor lógico entre 1 y 5 intentos máximo |
| codigo_postal_destino | STRING | Formato | Deben ser exactamente 5 dígitos numéricos (ej. 28001). |
| fecha_entrega | DATE | Freshness (Añadido en Soda) | Datos actualizados: la fecha máxima no puede tener más de 48h de antigüedad. |

# 🗂️ Tu espacio de trabajo en LOGIXPRESS

```bash
cd qg-logistica
docker compose up -d
```

💡 Consejo para el profesor: Presentar el ejercicio así cambia por completo la actitud de los alumnos. Ya no están haciendo un ejercicio de clase, están resolviendo una crisis de datos en Logixpress.

# 🚀 GUÍA DE LABORATORIO: Quality Gate en Logixpress Inc.

Soporte Técnico: Módulo de Calidad y Gobierno del Dato

Tiempo Estimado: 40 minutos

# 🗂️ PASO 1: Creación del Espacio de Trabajo (Local)

```bash
mkdir logixpress-quality-gate
cd logixpress-quality-gate

mkdir great_expectations_code
mkdir soda_code
```

## Estructura del proyecto

```text
logixpress-quality-gate/
├── docker-compose.yml
├── dataset.py
├── great_expectations_code/
│   └── script_gx.py
└── soda_code/
    ├── configuration.yml
    └── checks_logistica.yml
```

# 🐋 PASO 2: Orquestación del Entorno con Docker

## docker-compose.yml

```yaml
version: '3.8'
services:
 data-engine:
  image: jupyter/pyspark-notebook:latest
  container_name: logixpress_container
  ports:
   - "8888:8888"
  volumes:
   - .:/home/jovyan/work
  user: root
  command: >
   bash -c "pip install --no-cache-dir great-expectations soda-core-spark &&
   start-notebook.sh --NotebookApp.token=''"
```

## Arranque

```bash
docker compose up -d
```

# 📊 PASO 3: Inyección del Dataset "Sucio" (entregas_silver)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime, timedelta

def get_spark_dataset():
    spark = SparkSession.builder.appName("LogixpressQuality").getOrCreate()

    data = [
        ("E001", "P100", datetime.now().date(), "ENTREGADO", None, "28001", "Seur", 1),
        ("E002", "P101", datetime.now().date() - timedelta(days=3), "FALLIDO", "Dirección incorrecta", "08002", "MRW", 2),
        (None, "P102", datetime.now().date(), "PENDIENTE", None, "41003", "Correos", 1),
        ("E004", "P103", datetime.now().date(), "INVALIDO", None, "2800", "Seur", 3),
        ("E005", "P104", datetime.now().date(), "DEVUELTO", "Rechazado", "50005", "MRW", 6),
        ("E001", "P105", datetime.now().date(), "ENTREGADO", None, "28001", "Seur", 1)
    ]
```
(continúa con el schema y creación del DataFrame descritos en el documento)

# 🐍 PASO 4: Implementación con Great Expectations (Tarea 1)

Objetivo: Validar las 5 dimensiones DAMA mediante código imperativo en Python.

Incluye:
- Completitud de id_entrega e id_pedido.
- Unicidad de id_entrega.
- Validez de estado.
- Rango de intentos_entrega.
- Formato de codigo_postal_destino.
- Ejecución del checkpoint y evaluación final.

# 🥤 PASO 5: Implementación con Soda Core (Tarea 2)

## configuration.yml

```yaml
data_source logixpress_conn:
 type: spark
 connection_type: hive
```

## checks_logistica.yml

```yaml
checks for entregas_silver:

 - missing_count(id_entrega) = 0:
    name: ID Entrega obligatorio

 - missing_count(id_pedido) = 0:
    name: ID Pedido obligatorio

 - duplicate_count(id_entrega) = 0:
    name: ID Entrega clave primaria única

 - invalid_count(estado) = 0:
    valid values: ['ENTREGADO', 'FALLIDO', 'PENDIENTE', 'DEVUELTO']
    name: Estados permitidos por negocio

 - invalid_count(intentos_entrega) = 0:
    valid min: 1
    valid max: 5
    name: Rango de intentos permitido

 - invalid_count(codigo_postal_destino) = 0:
    valid format: regex ^\d{5}$
    name: Formato CP español de 5 dígitos

 - max(fecha_entrega) >= NOW - 48h:
    name: Alerta: La tabla lleva desactualizada más de 48 horas
```

# 🏃‍♂️ PASO 6: Ejecución y Evaluación en Clase

```bash
docker exec -it logixpress_container bash

cd work

python great_expectations_code/script_gx.py

soda scan -d logixpress_conn -c soda_code/configuration.yml soda_code/checks_logistica.yml
```

# 🧠 PASO 7: Sesión de Control de Daños (Análisis Crítico)

## Incidente

Un cliente de Logixpress llama furioso. Ha recibido un SMS diciendo que su paquete ha sido ENTREGADO, pero en la base de datos analítica el registro marca estado='FALLIDO'.

## Preguntas para debatir

### 1. ¿Qué check de los que hemos escrito hoy ha detectado este error?

Respuesta esperada: Ninguno. A nivel de base de datos, el valor FALLIDO es completamente válido, no es nulo y cumple el formato.

### 2. ¿Es un error de calidad del dato o un fallo de proceso?

Respuesta esperada: Es un fallo de proceso/sincronización de sistemas.

### 3. ¿Cómo lo solucionaríamos en el futuro?

Respuesta esperada: Implementando reglas de consistencia lógica cruzada (Cross-checks o Reconciliation Checks), cruzando la tabla de entregas con la tabla de logs de la pasarela de SMS.
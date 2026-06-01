"""Runner puente para Soda Core."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))
sys.path.append("/home/jovyan/work")

from dataset import get_spark_dataset  # noqa: E402
from soda.scan import Scan  # noqa: E402


def run_soda_scan() -> int:
    spark, df_entregas = get_spark_dataset()
    df_entregas.createOrReplaceTempView("entregas_silver")

    scan = Scan()
    # Usamos el nombre 'spark' que es el estándar interno
    ds_name = "spark"
    
    scan.set_data_source_name(ds_name)
    scan.add_configuration_yaml_file(str(PROJECT_ROOT / "soda_code" / "configuration.yml"))
    scan.add_sodacl_yaml_file(str(PROJECT_ROOT / "soda_code" / "checks_logistica.yml"))
    
    # Inyectamos la sesión. En algunas versiones de Soda, si el nombre es 'spark', 
    # se evita el error 'spark_df' porque el mapeo coincide con el tipo.
    scan.add_spark_session(spark, data_source_name=ds_name)

    exit_code = scan.execute()
    print(scan.get_logs_text())

    print("\n" + "=" * 70)
    print(f"CÓDIGO DE SALIDA SODA: {exit_code}")
    print("=" * 70 + "\n")
    return int(exit_code)


if __name__ == "__main__":
    run_soda_scan()
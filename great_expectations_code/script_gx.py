"""Quality Gate imperativo con Great Expectations para LOGIXPRESS."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))
# Ruta usada dentro del contenedor del documento original:
sys.path.append("/home/jovyan/work")

from dataset import get_spark_dataset  # noqa: E402
import great_expectations as gx  # noqa: E402


def run_quality_gate() -> bool:
    """Ejecuta el contrato de datos con Great Expectations."""
    spark, df_entregas = get_spark_dataset()

    context = gx.get_context()

    # 1. Configuración del Datasource (API Fluent)
    datasource_name = "logixpress_datasource"
    datasource = context.sources.add_or_update_spark(name=datasource_name)

    data_asset_name = "entregas_asset"
    try:
        data_asset = datasource.add_dataframe_asset(name=data_asset_name)
    except Exception:
        data_asset = datasource.get_asset(data_asset_name)

    batch_request = data_asset.build_batch_request(dataframe=df_entregas)

    # 2. Configuración de la Suite
    suite_name = "contracts_silver_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    # 3. Usamos un Validator para definir las expectativas
    # El validator conecta el batch de datos con la suite
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Añadir Expectativas usando el validator
    # Completitud
    validator.expect_column_values_to_not_be_null(column="id_entrega")
    validator.expect_column_values_to_not_be_null(column="id_pedido")

    # Unicidad
    validator.expect_column_values_to_be_unique(column="id_entrega")

    # Validez
    validator.expect_column_values_to_be_in_set(
        column="estado",
        value_set=["ENTREGADO", "FALLIDO", "PENDIENTE", "DEVUELTO"],
    )

    # Rango
    validator.expect_column_values_to_be_between(
        column="intentos_entrega",
        min_value=1,
        max_value=5,
    )

    # Formato
    validator.expect_column_values_to_match_regex(
        column="codigo_postal_destino",
        regex=r"^\d{5}$",
    )

    # Guardamos las expectativas en la suite
    validator.save_expectation_suite(discard_failed_expectations=False)

    # 4. Configuración y ejecución del Checkpoint para obtener el resultado final
    checkpoint = context.add_or_update_checkpoint(
        name="logixpress_gate",
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    resultado = checkpoint.run()

    print("\n" + "=" * 70)
    print(f"¿EL DATASET PASÓ EL QUALITY GATE DE GX?: {resultado.success}")
    print("=" * 70 + "\n")

    if not resultado.success:
        print("Resultado esperado: el dataset debe FALLAR porque contiene errores controlados.")
        print("Revisa el detalle generado por Great Expectations en el directorio gx/.")

    return bool(resultado.success)


if __name__ == "__main__":
    run_quality_gate()
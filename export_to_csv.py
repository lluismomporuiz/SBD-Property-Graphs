#!/usr/bin/env python3
"""
Script para exportar datos de DBLP XML a CSVs en formato Neo4j.
Crea automáticamente la carpeta de salida y ejecuta la conversión.
"""

import subprocess
import sys
import time
from pathlib import Path


def main():
    # Configuración de rutas
    project_root = Path(__file__).parent
    input_xml = project_root / "InputData" / "dblp.xml"
    input_dtd = project_root / "InputData" / "dblp.dtd"
    output_dir = project_root / "output_csv"
    output_base = output_dir / "dblp.csv"
    
    # Verificar que existen los archivos de entrada
    if not input_xml.exists():
        print(f"❌ Error: No se encuentra el archivo XML: {input_xml}")
        sys.exit(1)
    
    if not input_dtd.exists():
        print(f"❌ Error: No se encuentra el archivo DTD: {input_dtd}")
        sys.exit(1)
    
    # Crear carpeta de salida si no existe
    if not output_dir.exists():
        print(f"📁 Creando carpeta de salida: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        print(f"✓ La carpeta de salida ya existe: {output_dir}")
    
    # Comando a ejecutar
    print("\n🚀 Iniciando conversión de XML a CSV con formato Neo4j...")
    print(f"   - Archivo XML: {input_xml.name}")
    print(f"   - Archivo DTD: {input_dtd.name}")
    print(f"   - Carpeta destino: {output_dir.name}")
    print()
    
    command = [
        sys.executable,  # Usa el mismo Python que está ejecutando este script
        str(project_root / "xml_to_csv.py"),
        str(input_xml),
        str(input_dtd),
        str(output_base),
        "--neo4j"
    ]
    
    try:
        # Ejecutar el comando y medir el tiempo
        start_time = time.time()
        subprocess.run(command, check=True, capture_output=False, text=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print("\n✅ Conversión completada exitosamente!")
        print(f"⏱️  Tiempo total de conversión: {elapsed_time:.2f} segundos")
        print(f"\n📊 Los archivos CSV se encuentran en: {output_dir}")
        print("   - Estos archivos son compatibles con Neo4j")
        print("   - Puedes editarlos, filtrarlos o añadir datos adicionales")
        print("   - El script de importación para Neo4j se encuentra en: neo4j_import.sh")
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error durante la conversión: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"\n❌ Error: No se pudo ejecutar xml_to_csv.py")
        print("   Verifica que el archivo existe y que Python está instalado correctamente.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario")
        sys.exit(1)


if __name__ == "__main__":
    main()

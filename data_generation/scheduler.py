import sched
import time
import subprocess

scheduler = sched.scheduler(time.time, time.sleep)

# 🔁 Ejecutar dentro del contenedor spark-master
def run_in_container(script_name):
    try:
        result = subprocess.run(
            ["docker", "exec", "spark-master", "spark-submit", f"/opt/spark-apps/{script_name}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            timeout=120
        )
        if result.returncode == 0:
            print(f"{script_name} ejecutado correctamente:")
            if result.stdout:
                print(result.stdout)
        else:
            print(f"Error ejecutando {script_name}:")
            print(result.stderr)
    except subprocess.TimeoutExpired:
        print(f"⏱️ Tiempo de espera excedido ejecutando {script_name}")
    except Exception as e:
        print(f"🔥 Excepción inesperada ejecutando {script_name}: {e}")

# Generación de datos en Kafka (infinito, se ejecuta aparte manualmente)
# Esta parte no se programa con `sched`, ya que se lanza y queda en ejecución

# Extracción de datos (cada 60 segundos)
def schedule_extracion(sc): 
    run_in_container("integracion_kafka.py")
    sc.enter(60, 1, schedule_extracion, (sc,))

# Transformación de datos (cada 30 segundos)
def schedule_transformacion(sc): 
    run_in_container("transformacion_datos.py")
    sc.enter(30, 1, schedule_transformacion, (sc,))

# Carga de datos (cada 60 segundos)
def schedule_cargar(sc): 
    run_in_container("cargar_datos.py")
    sc.enter(60, 1, schedule_cargar, (sc,))

#  Análisis de datos (cada 120 segundos)
def schedule_analisis(sc):
    run_in_container("analisis_datos.py")
    sc.enter(120, 1, schedule_analisis, (sc,))

if __name__ == "__main__":
    print("Iniciando planificador...")
    scheduler.enter(0, 1, schedule_extracion, (scheduler,))
    scheduler.enter(5, 1, schedule_transformacion, (scheduler,))
    scheduler.enter(10, 1, schedule_cargar, (scheduler,))
    scheduler.enter(15, 1, schedule_analisis, (scheduler,)) 

    scheduler.run()

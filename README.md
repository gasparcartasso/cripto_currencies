# cripto_currencies

Este archivo describe las características principales del trabajo, los pasos necesarios para ejecutarlo y la estructura del repositorio.

## Motivacion y resumen del proyecto

Trackear monedas cripto para tomar decisiones de compra/venta.

Para ello se consultan dos APIs, una gratis pero poco estable y con limites de extraccion, y otra en caso de que falle la primera, paga.

La estructura de las base de datos es la primera en la cual se extrae la informacion y se carga con una minima transformacion a una base de datos. Luego se extrae la informacion de esa tabla base y se hacen transformaciones y agregaciones para tener una tabla mas pulida en la cual se puede consultar para generar dashboads.

Para almacenar los datos se uso un Redshift, y las dos tablas son DAILY_CRIPTO_PRICES (tabla en donde se hace la primer carga de datos) y FACT_CRIPTO_PRICES tabla en donde se hace la segunda carga de datos con mas informacion.

El proyecto tiene dos dags uno llamado 'extraction' para extraccion y guardado de datos semi-crudos y otro 'facttable' para la consulta, transformacion y guardado de datos transformados.

La extraccion de los datos es diaria de 4 cripto monedas, dado a que se pueden consultar datos historicos en la API al lanzar el airflow, estos se agregan al final de la base de datos y no son re escritos, sino que cuando se hace la base de datos FACT se filtran las ultimas entradas y se re escriben los datos en esa tabla FACT que es borrada y cargada de cero cada vez que este dag es lanzado.

Datos historicos fueron cargados a travez de CSVs una sola vez.

---

## Pasos para probar lanzar Airflow

1. **Clonar el repositorio**
   ```bash
   git clone https://github.com/gasparcartasso/cripto_currencies
   cd cripto_currencies
   ```
2. **Construir los servicios con Docker Compose**
    ```bash
    docker-compose up --build
    ```
3. **Acceder a la aplicación**
    http://localhost:8080
4. **Password y User**
    'airflow'
## Estructura del repositorio

docker-compose.yaml → Define los servicios, redes y volúmenes del proyecto.

Dockerfile → Imagen base para el servicio principal.

pyptoject.toml → Lista de dependencias necesarias.

dags/ → Código fuente del proyecto:

    API_request.py → funciones que van a ser usadas en los dags para consulta, carga y transformacion de datos.

    extraction.py → Consulta y carga los datos.

    facttable.py → Consulta a la base de datos raw, transformacion y carga de datos.

.github/workflows/test.yml → Conjunto de pruebas unitarias.

onetimeextraction.ipynb → Carga de datos historicos a la base de datos raw.

## Características del trabajo realizado
Integración con API externa para obtener datos en tiempo real.

Procesamiento y normalización de la información.

Pruebas unitarias incluidas.

Contenedor Docker + Docker Compose que permite ejecutar el proyecto de forma aislada y reproducible.

## Credenciales necesarias
API Key: requerida para acceder a los datos externos.

La clave no está incluida en el repositorio por motivos de seguridad.

Será enviada por privado (correo electrónico o Slack).

Debe colocarse en el archivo dags/.env.




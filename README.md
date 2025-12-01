# Siscom Trips Service

Servicio en Rust para evaluar trayectos de vehículos mediante mensajes MQTT.

## Requisitos

- Rust (latest stable)
- PostgreSQL
- Mosquitto (MQTT Broker)

## Configuración

El servicio se configura mediante variables de entorno. Se incluye un archivo `.env` de ejemplo.

```bash
cp .env.example .env
# Editar .env con sus credenciales
```

Variables principales:
- `MQTT_BROKER`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD`
- `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USER`, `DB_PWD`
- `LOG_LEVEL` (ej. `info`, `debug`)

## Base de Datos

Asegúrese de que las tablas existen en su base de datos PostgreSQL. Puede usar el archivo `schema.sql` para crearlas.

```bash
psql -h localhost -U siscom -d siscom_admin -f schema.sql
```

## Ejecución

Para ejecutar en modo desarrollo:

```bash
cargo run
```

Para compilar y ejecutar en producción (optimizado):

```bash
cargo build --release
./target/release/siscom-trips
```

## Estructura del Proyecto

- `src/main.rs`: Punto de entrada.
- `src/config.rs`: Carga de configuración.
- `src/mqtt.rs`: Cliente MQTT y loop de suscripción.
- `src/processor/message_processor.rs`: Lógica de negocio y transacciones.
- `src/db/`: Conexión a BD y queries.
- `src/models/`: Definición de estructuras de datos.

## Lógica de Procesamiento

El servicio consume mensajes JSON de MQTT, extrae `data` y `metadata`, y aplica las siguientes reglas de forma transaccional:

1. **Inicio de Trayecto**: `data.ALERT == "Engine On"`. Crea un nuevo `trip` si no existe uno activo.
2. **Fin de Trayecto**: `data.ALERT == "Engine Off"`. Cierra el `trip` activo.
3. **Puntos de Trayecto**: `MSG_CLASS == "STATUS"`. Inserta en `trip_points` si hay trip activo.
4. **Alertas**: Inserta siempre en `trip_alerts`.

Se utiliza `SELECT ... FOR UPDATE` para asegurar la consistencia y atomicidad por dispositivo.

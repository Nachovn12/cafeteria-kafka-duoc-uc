# Cafetería Kafka — Sistema de Pedidos en Tiempo Real

[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2+-blue.svg?logo=docker)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-8.3.1%20KRaft-red.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20%20Alpine-green.svg?logo=node.js)](https://nodejs.org/)
[![KafkaJS](https://img.shields.io/badge/KafkaJS-2.2.4-black.svg)](https://kafka.js.org/)
[![Express](https://img.shields.io/badge/Express-4.21.2-lightgrey.svg)](https://expressjs.com/)

Sistema integral de gestión de comandas y auditoría en tiempo real basado en una **Arquitectura Dirigida por Eventos (EDA)** con **Apache Kafka** en modo **KRaft**. La solución desacopla la captura de pedidos en caja de la preparación en cocina y la fiscalización analítica mediante microservicios orquestados en **Docker Compose**, utilizando **KafkaJS** y **Server-Sent Events (SSE)**.

---

## 1. Descripción General y Contexto

El proyecto modela el flujo operacional completo de una cafetería de especialidad moderna, resolviendo los desafíos críticos de concurrencia, escalabilidad y trazabilidad:

- **Baja latencia:** Notificación en milisegundos a las estaciones de cocina sin saturar al cliente HTTP.
- **Tolerancia a fallos:** Almacenamiento duradero e inmutable de cada transacción en el log distribuido de Kafka.
- **Afinidad de partición:** Preservación del orden secuencial de los pedidos de un mismo cliente mediante clave hash (`message.key`).
- **Escalamiento horizontal:** Balanceo automático de carga entre dos pantallas KDS en paralelo sin duplicidad de comandas.
- **Auditoría independiente:** Microservicio desacoplado con su propio Consumer Group para trazabilidad histórica integral desde el inicio del log (offset 0).
- **Resiliencia operativa:** Manejo defensivo contra cargas corruptas o mensajes mal formados sin colapso del sistema.

El sistema fue diseñado para ejecutarse en entornos educativos y de laboratorio del instituto Duoc UC, donde el puerto `8080` se encuentra típicamente reservado por Jenkins. Por ello, la aplicación expone los puertos externos **`8081`** (Tienda / Cocina 1) y **`8083`** (Cocina 2), garantizando compatibilidad total sin interferir con servicios preexistentes del host.

---

## 2. Objetivo Académico

Demostrar de forma práctica los principios rectores de sistemas distribuidos y mensajería orientada a eventos:
1. Publicación y confirmación física con metadatos reales de Kafka (partición y offset).
2. Transmisión reactiva en tiempo real sin polling activo.
3. Resiliencia y recuperación de comandas pendientes tras desconexión de consumidores.
4. Escalamiento horizontal con dos consumidores activos en el mismo Consumer Group (`grupo-cocina`), validando balanceo de particiones y ausencia de duplicados.
5. Consumo concurrente desacoplado mediante múltiples Consumer Groups independientes (`grupo-cocina` y `grupo-auditoria`).
6. Lectura histórica completa desde offset 0 sin alterar la posición de otros consumidores.
7. Particionamiento determinístico mediante hash de clave (`message.key`).
8. Manejo defensivo de errores y aislamiento de cargas no conformes.

---

## 3. Stack Tecnológico

| Capa / Componente | Tecnología | Rol en la Arquitectura |
| :--- | :--- | :--- |
| **Broker de Eventos** | Confluent cp-kafka 8.3.1 (KRaft) | Broker distribuido sin ZooKeeper. Escucha interna en `kafka:29092` y externa en host `localhost:9092`. |
| **Backend / API POS** | Node.js 20 (Alpine) + Express | API REST de caja y orquestador del productor/consumidor KafkaJS. Expuesto en puerto `:8081`. |
| **Cocina 2 (Escalado)** | Node.js 20 (Alpine) + Express | Segunda instancia KDS para balanceo horizontal de particiones. Expuesto en puerto `:8083`. |
| **Auditoría** | Node.js 20 (Alpine) Headless | Microservicio independiente para fiscalización, trazabilidad histórica y control de anomalías. |
| **Cliente Kafka** | KafkaJS 2.2.4 | Driver nativo para publicación (`producer`) y consumo en grupo (`consumer`). |
| **Streaming Web** | Server-Sent Events (SSE) | Transmisión unidireccional HTTP push desde Express hacia los monitores KDS de Cocina. |
| **Frontend** | HTML5, CSS3 Vanilla, JavaScript ES6+ | Interfaces operativas POS (Caja) y KDS (Cocina), responsive y accesibles. |
| **Design System** | Specialty Coffee Operations Tokens | Paleta tonal cálida, tipografía tabular y microinteracciones baristas. |
| **Iconografía** | Lucide Icons (Render SVG embebido) | Simbología semántica para estados, alertas y categorías de producto. |
| **Orquestación** | Docker Compose v2+ | Despliegue reproducible de los 4 servicios con un solo comando sin requerir Node en el host. |

---

## 4. Arquitectura Basada en Eventos (EDA) y Consumer Groups

El topic `pedidos` cuenta con **3 particiones físicas** que son consumidas concurrentemente por dos grupos consumidores completamente independientes:

```text
 [ Tienda POS ]
 (tienda.html :8081)
      │
      │  HTTP POST /api/pedidos
      ▼
 [ Express Server 1 ] ──► producer.send({ topic: 'pedidos', key: cliente })
 (cafeteria-frontend)                      │
      │                                    ▼
      │  HTTP 200 { particion, offset }  [ Apache Kafka KRaft ]
      ▼                                  Topic: 'pedidos' (3 Particiones)
 [ Comprobante Oficial ]                 ┌──────────────┬──────────────┬──────────────┐
                                         │ Partición 0  │ Partición 1  │ Partición 2  │
                                         └──────────────┴──────────────┴──────────────┘
                                                │              │              │
       ═════════════════════════════════════════╪══════════════╪══════════════╪═════════════════════════════════════════
      ║                                         │              │              │                                           ║
      ║  CONSUMER GROUP 1: 'grupo-cocina'       │              │              │                                           ║
      ║  (Balanceo de carga entre cocinas)      │              │              │                                           ║
      ║             ┌───────────────────────────┘              │              └───────────────────────────┐               ║
      ║             │                                          │                                          │               ║
      ║             ▼                                          ▼                                          ▼               ║
      ║  [ Consumer 1 (grupo-cocina) ]                                         [ Consumer 2 (grupo-cocina) ]              ║
      ║  (cafeteria-frontend :8081)                                            (cafeteria-frontend-2 :8083)               ║
      ║  Asignación: Particiones 0 y 2                                         Asignación: Partición 1                    ║
      ║             │                                                                     │                               ║
      ║             │  SSE push (P0 + P2)                                                 │  SSE push (P1)                ║
      ║             ▼                                                                     ▼                               ║
      ║    [ Cocina 1 KDS ]                                                      [ Cocina 2 KDS ]                         ║
      ║  (http://localhost:8081/cocina)                                        (http://localhost:8083/cocina)             ║
      ║  13 pedidos / 0 duplicados                                             7 pedidos / 0 duplicados                   ║
      ║                                                                                                                   ║
       ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
      ║                                                                                                                   ║
      ║  CONSUMER GROUP 2: 'grupo-auditoria' (Microservicio Desacoplado)                                                  ║
      ║  (Fan-out total: Lee el 100% de los mensajes desde Offset 0 sin interferir con Cocina)                            ║
      ║                                                                                                                   ║
      ║  [ Microservicio Auditoría (cafeteria-auditoria) ]                                                                ║
      ║  Asignación: Particiones 0, 1 y 2 (fromBeginning: true)                                                           ║
      ║             │                                                                                                     ║
      ║             ▼                                                                                                     ║
      ║    [ Log Stream de Trazabilidad ]                                                                                 ║
      ║    Lectura histórica completa (P0, P1, P2) &bull; Detección R7 &bull; LAG 0 &bull; Sin impacto en Cocina          ║
      ║                                                                                                                   ║
       ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
```

---

## 5. Estructura del Repositorio

```text
cafeteria-kafka-duoc-uc/
├── docker-compose.yml          # Definición de los 4 servicios: kafka, frontend, frontend-2 y auditoria
├── run_verification.ps1        # Script de prueba rápida en PowerShell (R1, R2, R6, R7)
├── test_integration.js         # Suite automatizada de integración contra el stack Docker
├── skills-lock.json            # Bloqueo de versiones de habilidades de agentes
├── .gitignore                  # Exclusiones de Git (node_modules, logs, agentes)
├── README.md                   # Documentación técnica integral del proyecto
│
├── tests/                      # Suites de prueba especializadas
│   └── test_r4.js              # Suite de validación automatizada de R4 (Dos Cocinas KDS)
│
├── evidencias/                 # Registro fotográfico y capturas de laboratorio
│   ├── 01-laboratorio-kafka/   # E01 - E05 (Particiones, consumer groups, LAG)
│   └── 02-caso-cafeteria/      # R1, R2, R3, R4, R5, R6, R7 (Evidencias validadas)
│       ├── R1_Publicacion_Comprobante.png
│       ├── R1_Verificacion_Kafka.png
│       ├── R2_Pedido_Tiempo_Real.png
│       ├── R3_LAG_Cocina_Detenida.png
│       ├── R3_Recuperacion_5_Pedidos.png
│       ├── R4_Cocina_1_13_Pedidos.png
│       ├── R4_Cocina_2_7_Pedidos.png
│       ├── R5_Auditoria_Historial_Offset0.png
│       ├── R5_Cocina_1_Pedido_Auditado.png
│       ├── R5_Cocina_2_Pedido_Auditado.png
│       ├── R5_Consumer_Groups_Independientes.png
│       ├── R6_Orden_Particiones.png
│       └── R7_Mensaje_Malformado_Resiliencia.png
│
├── auditoria/                  # Microservicio de Auditoría Independiente (R5)
│   ├── Dockerfile              # Construcción Alpine headless para auditoría
│   ├── .dockerignore           # Exclusión de node_modules
│   ├── package.json            # Metadatos y dependencia KafkaJS
│   ├── package-lock.json       # Árbol de dependencias inmutable
│   └── server.js               # Lógica de consumo en 'grupo-auditoria' y log de control
│
└── frontend/                   # Aplicación Node.js (Frontend POS/KDS + Backend API)
    ├── Dockerfile              # Construcción multi-etapa con node:20-alpine
    ├── .dockerignore           # Exclusión de node_modules en build de Docker
    ├── package.json            # Metadatos del proyecto y dependencias fijadas
    ├── package-lock.json       # Árbol de dependencias inmutable para npm ci
    ├── server.js               # Servidor Express, productor/consumidor KafkaJS y SSE
    └── public/                 # Archivos estáticos servidos al navegador
        ├── tienda.html         # Terminal de Punto de Venta (POS)
        ├── cocina.html         # Monitor de Cocina en Tiempo Real (KDS)
        ├── css/
        │   └── styles.css      # Design System completo y estilos de componentes
        └── js/
            ├── tienda.js       # Lógica de formulario, presets y comprobante POS
            ├── cocina.js       # Suscripción SSE, deduplicación y renderizado KDS
            └── icons.js        # Librería auxiliar de iconos Lucide SVG
```

---

## 6. Requisitos Previos

Únicamente se requiere tener instalado y en ejecución:
- **Docker Desktop** (versión 4.20+ con Docker Compose v2+).
- **Git** (para clonar el repositorio).

> [!NOTE]
> No es necesario tener Node.js ni npm instalados en el sistema operativo Windows; la construcción y ejecución ocurren íntegramente dentro de los contenedores Docker.

---

## 7. Instalación y Puesta en Marcha

### 7.1. Clonar el repositorio
```bash
git clone https://github.com/Nachovn12/cafeteria-kafka-duoc-uc.git
cd cafeteria-kafka-duoc-uc
```

### 7.2. Ejecutar mediante un único comando
```bash
docker compose up -d --build
```

El proceso compila las imágenes necesarias con dependencias seguras (`npm ci --omit=dev`) y levanta los **cuatro servicios del ecosistema**:
1. `kafka`: Broker Apache Kafka 8.3.1 KRaft.
2. `frontend`: Tienda POS y Cocina 1 KDS (`:8081`).
3. `frontend-2`: Cocina 2 KDS para balanceo horizontal (`:8083`).
4. `auditoria`: Microservicio headless de fiscalización continua.

---

## 8. Puntos de Entrada y URLs de la Aplicación

| Módulo | URL de Acceso | Contenedor | Descripción |
| :--- | :--- | :--- | :--- |
| **Tienda (POS)** | `http://localhost:8081/` | `cafeteria-frontend` | Punto de venta: selección de café, cantidad, notas rápidas y comprobante digital. |
| **Cocina 1 (KDS)** | `http://localhost:8081/cocina` | `cafeteria-frontend` | Monitor KDS principal (consume Particiones 0 y 2 del topic `pedidos`). |
| **Cocina 2 (KDS)** | `http://localhost:8083/cocina` | `cafeteria-frontend-2` | Monitor KDS secundario (consume Partición 1 del topic `pedidos`). |
| **Healthcheck 1** | `http://localhost:8081/api/health` | `cafeteria-frontend` | Diagnóstico de salud y estado de conexión KafkaJS de la Instancia 1. |
| **Healthcheck 2** | `http://localhost:8083/api/health` | `cafeteria-frontend-2` | Diagnóstico de salud y estado de conexión KafkaJS de la Instancia 2. |
| **Canal SSE 1** | `http://localhost:8081/api/eventos` | `cafeteria-frontend` | Flujo Server-Sent Events hacia el monitor KDS de Cocina 1. |
| **Canal SSE 2** | `http://localhost:8083/api/eventos` | `cafeteria-frontend-2` | Flujo Server-Sent Events hacia el monitor KDS de Cocina 2. |
| **Auditoría Log** | `docker logs -f cafeteria-auditoria` | `cafeteria-auditoria` | Salida en consola de todas las transacciones auditadas desde offset 0. |

---

## 9. Conceptos Clave de Apache Kafka en este Proyecto

- **Broker (`kafka:29092` interno / `localhost:9092` host):** Servidor central de mensajería ejecutado en modo KRaft (sin ZooKeeper), coordinando líderes y particiones.
- **Topic (`pedidos`):** Canal temático configurado con **3 particiones** físicas y factor de replicación 1.
- **Particiones (`0`, `1`, `2`):** Unidades de paralelismo y almacenamiento secuencial dentro del broker.
- **Clave (`message.key`):** Se utiliza el **nombre del cliente** como clave para garantizar que todos los pedidos del mismo cliente se dirijan siempre a la misma partición física.
- **Offset:** Identificador numérico correlativo asignado por Kafka a cada mensaje dentro de una partición. Es inmutable y no reutilizable.
- **Producer:** Instancia KafkaJS en `server.js` con particionador determinístico (`DefaultPartitioner` basado en hash murmur2).

### 9.1. Diferencia Fundamental entre Consumer Groups
- **`grupo-cocina` (Consumidores Competidores):** Agrupa a las instancias `frontend` y `frontend-2`. Las 3 particiones se dividen dinámicamente entre ambas pantallas (P0 y P2 a Cocina 1; P1 a Cocina 2). Cada mensaje es procesado por **una sola cocina**, evitando la preparación duplicada de alimentos.
- **`grupo-auditoria` (Consumidor Independiente / Fan-Out):** No compite con las cocinas. Al pertenecer a un grupo distinto, Kafka le entrega **el 100% de los mensajes de las 3 particiones**. La velocidad de consumo de auditoría no retrasa ni bloquea a la cocina, demostrando el desacoplamiento total de la arquitectura EDA.

### 9.2. Semántica de Lectura Histórica (`fromBeginning: true`)
- Al arrancar un consumidor con `fromBeginning: true`:
  - **Si el Consumer Group es nuevo** (sin registros previos en el topic interno `__consumer_offsets`): Kafka inicia la lectura desde el **offset 0** de cada partición, recuperando todo el historial existente.
  - **Si el Consumer Group ya posee offsets confirmados (commits previos):** Kafka respeta la posición guardada y **reanuda la lectura exclusivamente a partir del último offset confirmado**, garantizando semántica de procesamiento sin duplicidad tras caídas o reinicios.

---

## 10. Contratos de la API y Formato de Eventos

### 10.1. API REST — Registro de Comanda
**Endpoint:** `POST /api/pedidos`  
**Payload de entrada:**
```json
{
  "cliente": "Ana Valenzuela",
  "producto": "Café Latte Doble",
  "cantidad": 2,
  "nota": "Leche de avena, sin azúcar"
}
```

**Respuesta exitosa (HTTP 200 / 201) con metadatos reales de Kafka:**
```json
{
  "resultados": [
    {
      "pedido": {
        "id": "A3F2",
        "cliente": "Ana Valenzuela",
        "producto": "Café Latte Doble",
        "cantidad": 2,
        "nota": "Leche de avena, sin azúcar",
        "creado_en": "2026-10-29T19:01:52.021Z"
      },
      "particion": 0,
      "offset": 22
    }
  ]
}
```

### 10.2. Server-Sent Events (SSE) — Transmisión a Cocina
**Endpoint:** `GET /api/eventos`  
**Evento de comanda regular:**
```json
{
  "tipo": "pedido",
  "valido": true,
  "pedido": {
    "id": "A3F2",
    "cliente": "Ana Valenzuela",
    "producto": "Café Latte Doble",
    "cantidad": 2,
    "nota": "Leche de avena, sin azúcar",
    "creado_en": "2026-10-29T19:01:52.021Z"
  },
  "particion": 0,
  "offset": 22,
  "key": "Ana Valenzuela",
  "timestamp": "2026-10-29T19:01:52.050Z",
  "historico": false,
  "latencia_ms": 29
}
```

**Evento de mensaje mal formado (Requisito R7):**
```json
{
  "tipo": "malformado",
  "valido": false,
  "mensaje": "No es un pedido",
  "raw": "hola",
  "particion": 1,
  "offset": 4,
  "key": null,
  "timestamp": "2026-10-29T19:01:54.002Z",
  "historico": false
}
```

---

## 11. Estado de Requisitos Funcionales (R1 – R7)

| Requisito | Descripción | Estado | Detalle y Evidencia |
| :---: | :--- | :---: | :--- |
| **R1** | Publicación desde POS con comprobante real | **Aprobado** | Publicación en topic `pedidos` con confirmación de ACK del broker, retornando partición y offset reales en el comprobante digital. Evidencias: `R1_Publicacion_Comprobante.png` y `R1_Verificacion_Kafka.png`. |
| **R2** | Visualización en vivo en Cocina (< 2 segundos) | **Aprobado** | Transmisión reactiva vía SSE. La latencia de transporte medida entre el POST y el despacho SSE oscila entre 5 ms y 35 ms. La visualización en pantalla añade únicamente el tiempo de renderizado del navegador ($< 16\text{ ms}$ a 60fps), completando el flujo total en una fracción mínima del límite de 2 segundos. Evidencia: `R2_Pedido_Tiempo_Real.png`. |
| **R3** | Persistencia y recuperación tras reinicio de Cocina | **Aprobado** | La cocina recupera comandas acumuladas mediante reanudación de offsets en Kafka y buffer en memoria del servidor. Evidencias: `R3_LAG_Cocina_Detenida.png` y `R3_Recuperacion_5_Pedidos.png`. |
| **R4** | Escalamiento horizontal: Dos Cocinas KDS | **Aprobado** | Dos instancias (`:8081` y `:8083`) en `grupo-cocina`. Validación con 20 pedidos únicos: Cocina 1 procesó 13 pedidos (P0 y P2); Cocina 2 procesó 7 pedidos (P1). **Cero duplicados, cero faltantes, LAG final en 0**. Evidencias: `R4_Cocina_1_13_Pedidos.png` y `R4_Cocina_2_7_Pedidos.png`. |
| **R5** | Microservicio de Auditoría Independiente | **Aprobado** | Microservicio `cafeteria-auditoria` con Consumer Group propio (`grupo-auditoria`). Lectura histórica desde offset 0 en las 3 particiones, detección de mensajes corruptos, procesamiento simultáneo e independiente de pedidos (`R5-001` en Cocina 1 y Auditoría; `R5-002` en Cocina 2 y Auditoría) sin interferir en cocina. LAG final en 0. Evidencias: `R5_Auditoria_Historial_Offset0.png`, `R5_Cocina_1_Pedido_Auditado.png`, `R5_Cocina_2_Pedido_Auditado.png`, `R5_Consumer_Groups_Independientes.png`. |
| **R6** | Afinidad de partición y preservación de orden por clave | **Aprobado** | Validado: pedidos sucesivos del mismo cliente aterrizan en la misma partición física manteniendo el orden correlativo estricto. Evidencia: `R6_Orden_Particiones.png`. |
| **R7** | Tolerancia a mensajes mal formados (No es un pedido) | **Aprobado** | Mensajes de texto plano (ej. `hola`) son clasificados como malformados sin colapsar el consumidor ni permitir inyección HTML. Evidencia: `R7_Mensaje_Malformado_Resiliencia.png`. |
| **R8** | Filtro temático por rol de preparación | *Opcional* | Requisito complementario opcional; no implementado en esta entrega académica. |

---

## 12. Comandos de Gestión y Diagnóstico

### Estado de los Cuatro Contenedores
```bash
docker compose ps
```

### Ver Logs de Cocina 1, Cocina 2 y Auditoría en Tiempo Real
```bash
# Cocina 1
docker logs -f cafeteria-frontend

# Cocina 2
docker logs -f cafeteria-frontend-2

# Microservicio de Auditoría (Trazabilidad R5)
docker logs -f cafeteria-auditoria
```

### Ver Logs del Broker Kafka
```bash
docker logs -f kafka
```

### Diagnóstico de Salud de las Instancias Web
```bash
curl -s http://localhost:8081/api/health
curl -s http://localhost:8083/api/health
```

### Inspeccionar Particiones del Topic `pedidos`
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic pedidos
```

### Inspeccionar Ambos Consumer Groups en el Broker
```bash
# Inspeccionar Grupo Cocina (Particiones divididas entre Cocina 1 y 2)
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group grupo-cocina

# Inspeccionar Grupo Auditoría (Todas las particiones asignadas)
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group grupo-auditoria
```

---

## 13. Ejecución de Pruebas Automatizadas

### 13.1. Suite General de Integración (R1, R2, R6 y R7)
Valida la publicación, latencia SSE, afinidad de clave y tolerancia a fallos:
```powershell
# Mediante PowerShell
powershell -ExecutionPolicy Bypass -File .\run_verification.ps1

# O directamente con Node.js
node test_integration.js
```

### 13.2. Suite de Escalamiento Horizontal R4 (Dos Cocinas)
Valida la recepción simultánea de 20 pedidos repartidos entre Cocina 1 (:8081) y Cocina 2 (:8083):
```bash
node tests/test_r4.js
```

> [!WARNING]
> **Efecto de la ejecución de `test_r4.js` en Kafka:**
> El script publica un lote de 20 pedidos de prueba (`R4-001` a `R4-020`) en el topic `pedidos`, lo que avanza los offsets de las 3 particiones y añade registros al historial del broker. Se recomienda ejecutar este script únicamente cuando se requiera revalidar el balanceo de carga entre ambas instancias de cocina.

---

## 14. Limitaciones Técnicas Conocidas

1. **Puertos externos 8081 y 8083:** Se utilizan `8081` y `8083` para evitar colisiones con Jenkins (puerto `8080` de los equipos de laboratorio). Si se requiere otro puerto para la tienda, puede sobreescribirse mediante la variable `APP_PORT=xxxx docker compose up -d`.
2. **Buffer de reconexión SSE en memoria:** Cada instancia Express mantiene su propio buffer de eventos recientes en memoria; ante un reinicio del contenedor, las nuevas comandas se sincronizan directamente desde el broker Kafka.
3. **Persistencia de Kafka y Volumen Anónimo:** Los datos del clúster KRaft residen actualmente en el volumen anónimo asignado al contenedor `kafka`. Por este motivo, **nunca debe ejecutarse `docker compose down -v`**, ya que eliminaría el volumen y el historial completo de mensajes y offsets del laboratorio.
4. **Compatibilidad de Versiones de Node.js:** En Docker se ejecuta Node.js 20 Alpine con máxima estabilidad. Si se ejecuta el proyecto localmente sobre Windows con versiones de Node.js 22+ o 25+, KafkaJS puede emitir una advertencia no bloqueante de temporizadores (`TimeoutNegativeWarning`), la cual no afecta la conectividad ni el procesamiento de mensajes.

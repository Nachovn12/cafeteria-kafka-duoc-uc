const express = require('express');
const path = require('path');
const { Kafka, Partitioners, logLevel } = require('kafkajs');

const app = express();
const PORT = process.env.PORT || 3000;
const rawBrokers = process.env.KAFKA_BROKERS || 'localhost:9092';
const kafkaBrokers = rawBrokers.split(',').map(b => b.trim()).filter(Boolean);

// Marca de tiempo de arranque del servidor para distinguir mensajes históricos de nuevos pedidos
const serverStartTime = Date.now();

// Middleware para procesar cuerpos JSON y servir archivos estáticos
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Clientes SSE conectados para transmisión en tiempo real a la cocina
const sseClients = new Set();

// Buffer acotado de eventos para recuperación y consistencia de la Cocina (Fase 4)
const BUFFER_MAX_SIZE = 100;
const recentEventsBuffer = [];

/**
 * Almacena eventos recientes en memoria evitando duplicados por partición y offset
 * @param {Object} evento - Objeto de evento normalizado
 */
function guardarEnBuffer(evento) {
  const claveUnica = `${evento.particion}-${evento.offset}`;
  const existe = recentEventsBuffer.some(e => `${e.particion}-${e.offset}` === claveUnica);
  if (!existe) {
    if (recentEventsBuffer.length >= BUFFER_MAX_SIZE) {
      recentEventsBuffer.shift();
    }
    recentEventsBuffer.push(evento);
  }
}

/**
 * Difunde un evento a todas las interfaces de cocina conectadas vía SSE
 * @param {Object} evento - Datos a enviar al frontend de cocina
 */
function enviarEventoACocina(evento) {
  const payload = `data: ${JSON.stringify(evento)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch (err) {
      console.warn('⚠️ Error al transmitir a cliente SSE:', err.message);
      sseClients.delete(client);
    }
  }
}

// ============================================================================
// INTEGRACIÓN KAFKAJS: IMPLEMENTACIÓN DE TODOs 1 AL 7
// ============================================================================

// TODO 1: Configurar cliente de KafkaJS con clientId 'cafeteria-app' y brokers configurables
const kafka = new Kafka({
  clientId: 'cafeteria-app',
  brokers: kafkaBrokers,
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 300,
    retries: 8
  }
});

/**
 * Fase 3: Espera a que Kafka esté listo y asegura que el topic 'pedidos' exista con 3 particiones
 * sin modificar topics existentes ni borrar offsets/mensajes.
 */
async function asegurarInfraestructuraKafka(maxIntentos = 30, intervaloMs = 2000) {
  const admin = kafka.admin();
  let intento = 0;
  let conectado = false;

  while (!conectado && intento < maxIntentos) {
    intento++;
    try {
      console.log(`[Kafka Admin] Verificando conexión con broker (intento ${intento}/${maxIntentos})...`);
      await admin.connect();
      conectado = true;
      console.log('✅ [Kafka Admin] Conexión establecida con el broker Kafka.');
    } catch (err) {
      console.warn(`⏳ [Kafka Admin] Broker no disponible aún (${err.message}). Reintentando en ${intervaloMs / 1000}s...`);
      await new Promise(r => setTimeout(r, intervaloMs));
    }
  }

  if (!conectado) {
    throw new Error(`No fue posible conectar con Kafka tras ${maxIntentos} intentos.`);
  }

  try {
    const topics = await admin.listTopics();
    const existeTopic = topics.includes('pedidos');

    if (!existeTopic) {
      console.log("ℹ️ [Kafka Admin] Topic 'pedidos' no existe. Creando con 3 particiones y factor de replicación 1...");
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: 'pedidos',
            numPartitions: 3,
            replicationFactor: 1
          }
        ]
      });
      console.log("✅ [Kafka Admin] Topic 'pedidos' creado exitosamente con 3 particiones.");
    } else {
      const metadata = await admin.fetchTopicMetadata({ topics: ['pedidos'] });
      const infoTopic = metadata.topics.find(t => t.name === 'pedidos');
      const cantParticiones = infoTopic ? infoTopic.partitions.length : 'desconocido';
      console.log(`✅ [Kafka Admin] Topic 'pedidos' ya existe con ${cantParticiones} particiones. Datos históricos preservados.`);
    }
  } finally {
    await admin.disconnect();
  }
}

// Variable para el productor (TODO 2)
let producer = null;
let producerConectado = false;

// TODO 2: Inicializar y conectar Productor de Kafka
async function inicializarProductor() {
  producer = kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner
  });

  producer.on('producer.connect', () => {
    producerConectado = true;
    console.log(`✅ [Kafka Producer] Conectado exitosamente a brokers: ${kafkaBrokers.join(', ')}`);
  });

  producer.on('producer.disconnect', () => {
    producerConectado = false;
    console.warn('⚠️ [Kafka Producer] Productor desconectado del broker');
  });

  await producer.connect();
  producerConectado = true;
  console.log(`✅ [Kafka Producer] Listo para despachar pedidos en topic 'pedidos'`);
}

// Variable para el consumidor (TODO 5)
let consumer = null;
let consumerConectado = false;

/**
 * Helper para validar pedidos estructurados según las reglas del negocio
 * @param {any} obj - Objeto a validar
 * @returns {boolean}
 */
function esPedidoEstructuradoValido(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    return false;
  }
  if (!obj.cliente || typeof obj.cliente !== 'string' || !obj.cliente.trim()) {
    return false;
  }
  if (!obj.producto || typeof obj.producto !== 'string' || !obj.producto.trim()) {
    return false;
  }
  const cantidad = Number(obj.cantidad);
  if (isNaN(cantidad) || cantidad <= 0) {
    return false;
  }
  return true;
}

// TODO 5: Inicializar y conectar Consumidor de Kafka con groupId 'grupo-cocina'
// TODO 6: Suscribir Consumidor a topic 'pedidos' con fromBeginning: true
// TODO 7: Ejecutar consumidor (consumer.run), manejar errores/R7 y retransmitir eventos con enviarEventoACocina
async function inicializarConsumidor() {
  consumer = kafka.consumer({
    groupId: 'grupo-cocina',
    retry: {
      initialRetryTime: 300,
      retries: 10
    }
  });

  consumer.on('consumer.connect', () => {
    consumerConectado = true;
    console.log(`✅ [Kafka Consumer] Conectado al broker con groupId 'grupo-cocina'`);
  });

  consumer.on('consumer.disconnect', () => {
    consumerConectado = false;
    console.warn(`⚠️ [Kafka Consumer] Consumidor desconectado del broker`);
  });

  consumer.on('consumer.crash', (error) => {
    consumerConectado = false;
    console.error(`💥 [Kafka Consumer] Error fatal en consumidor:`, error);
  });

  await consumer.connect();
  consumerConectado = true;

  // TODO 6: Suscribir al topic 'pedidos' con fromBeginning: true
  // Los offsets confirmados previamente prevalecen; si el grupo es nuevo lee desde el inicio
  await consumer.subscribe({
    topic: 'pedidos',
    fromBeginning: true
  });
  console.log(`✅ [Kafka Consumer] Suscrito a topic 'pedidos' (fromBeginning: true)`);

  // TODO 7: Procesamiento de cada mensaje
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const offset = Number(message.offset);
        const rawValue = message.value ? message.value.toString('utf-8') : '';
        const key = message.key ? message.key.toString('utf-8') : null;
        const kafkaTimestamp = message.timestamp ? Number(message.timestamp) : Date.now();
        const esHistorico = kafkaTimestamp < serverStartTime;

        let parsed = null;
        let esValido = false;

        try {
          parsed = JSON.parse(rawValue);
          esValido = esPedidoEstructuradoValido(parsed);
        } catch (jsonErr) {
          esValido = false;
        }

        if (esValido) {
          // Pedido válido
          const pedidoNormalizado = {
            id: parsed.id || `P-${partition}-${offset}`,
            cliente: String(parsed.cliente).trim(),
            producto: String(parsed.producto).trim(),
            cantidad: Number(parsed.cantidad) || 1,
            nota: (parsed.nota || '').trim(),
            creado_en: parsed.creado_en || new Date(kafkaTimestamp).toISOString()
          };

          const ahora = Date.now();
          const latenciaMs = pedidoNormalizado.creado_en
            ? Math.max(0, ahora - new Date(pedidoNormalizado.creado_en).getTime())
            : 0;

          const eventoSse = {
            tipo: 'pedido',
            valido: true,
            pedido: pedidoNormalizado,
            particion: partition,
            offset: offset,
            key: key,
            timestamp: new Date().toISOString(),
            historico: esHistorico,
            latencia_ms: latenciaMs
          };

          guardarEnBuffer(eventoSse);
          enviarEventoACocina(eventoSse);

          const etiqueta = esHistorico ? '[HISTÓRICO]' : '[NUEVO]';
          console.log(`📥 [Cocina] ${etiqueta} Pedido #${pedidoNormalizado.id} en P${partition}@${offset} (Cliente: "${pedidoNormalizado.cliente}")`);
        } else {
          // Mensaje mal formado o no es un pedido (Requisito R7)
          const eventoMalformado = {
            tipo: 'malformado',
            valido: false,
            mensaje: 'No es un pedido',
            raw: rawValue.substring(0, 500),
            particion: partition,
            offset: offset,
            key: key,
            timestamp: new Date().toISOString(),
            historico: esHistorico
          };

          guardarEnBuffer(eventoMalformado);
          enviarEventoACocina(eventoMalformado);
          console.warn(`⚠️ [Cocina] Mensaje recibido en P${partition}@${offset} clasificado como 'No es un pedido' (Raw: "${rawValue.substring(0, 60)}")`);
        }
      } catch (errProcesamiento) {
        console.error('❌ Error controlado en eachMessage (consumidor protegido):', errProcesamiento);
      }
    }
  });
}

// ============================================================================
// RUTAS DE LA APLICACIÓN
// ============================================================================

/**
 * GET / -> Vista de la Tienda (punto de venta)
 */
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'tienda.html'));
});

/**
 * GET /cocina -> Vista de la Cocina (monitor de comandas)
 */
app.get('/cocina', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'cocina.html'));
});

/**
 * GET /api/health -> Estado diagnóstico de servidor y Kafka
 */
app.get('/api/health', (req, res) => {
  res.json({
    status: (producerConectado && consumerConectado) ? 'ok' : 'degraded',
    servicio: 'cafeteria-frontend',
    uptime: Math.round(process.uptime()),
    timestamp: new Date().toISOString(),
    kafka: {
      brokers: kafkaBrokers,
      productorConectado: producerConectado,
      consumidorConectado: consumerConectado,
      eventosEnBuffer: recentEventsBuffer.length,
      clientesSseConectados: sseClients.size
    }
  });
});

/**
 * GET /api/eventos -> Canal SSE para eventos en tiempo real hacia la Cocina
 */
app.get('/api/eventos', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  // Registrar cliente SSE
  sseClients.add(res);

  // Mensaje de handshake inicial con metadatos del servidor
  const saludo = {
    tipo: 'conexion',
    mensaje: 'Conexión SSE establecida con el servidor de Cafetería',
    timestamp: new Date().toISOString(),
    bufferSize: recentEventsBuffer.length
  };
  res.write(`data: ${JSON.stringify(saludo)}\n\n`);

  // Fase 4: Replay de eventos en buffer para que la cocina reciba comandas ya consumidas
  for (const evento of recentEventsBuffer) {
    res.write(`data: ${JSON.stringify(evento)}\n\n`);
  }

  // Heartbeat periódico para mantener viva la conexión HTTP SSE
  const heartbeatInterval = setInterval(() => {
    res.write(': ping\n\n');
  }, 25000);

  // Limpiar recursos al desconectar
  req.on('close', () => {
    clearInterval(heartbeatInterval);
    sseClients.delete(res);
  });
});

/**
 * POST /api/pedidos -> Recepción de pedidos desde la Tienda (R1 y R6)
 *
 * Contrato exigido:
 * {
 *   "resultados": [
 *     {
 *       "pedido": { id, cliente, producto, cantidad, nota, creado_en },
 *       "particion": 0,
 *       "offset": 12
 *     }
 *   ]
 * }
 */
app.post('/api/pedidos', async (req, res) => {
  if (!producer || !producerConectado) {
    return res.status(503).json({
      ok: false,
      error: 'Productor Kafka no disponible',
      mensaje: 'El servicio de mensajería Kafka no se encuentra conectado actualmente.'
    });
  }

  const { cliente, producto, cantidad, nota } = req.body;

  // Validación rigurosa de campos obligatorios
  if (!cliente || typeof cliente !== 'string' || !cliente.trim()) {
    return res.status(400).json({
      error: 'Datos de pedido inválidos',
      mensaje: 'El campo "cliente" es obligatorio.'
    });
  }

  if (!producto || typeof producto !== 'string' || !producto.trim()) {
    return res.status(400).json({
      error: 'Datos de pedido inválidos',
      mensaje: 'El campo "producto" es obligatorio.'
    });
  }

  const numCantidad = Number(cantidad);
  if (isNaN(numCantidad) || numCantidad <= 0) {
    return res.status(400).json({
      error: 'Datos de pedido inválidos',
      mensaje: 'El campo "cantidad" debe ser un número entero mayor a 0.'
    });
  }

  // Estructura normalizada del pedido según contrato exigido
  const pedido = {
    id: req.body.id || Math.random().toString(16).substring(2, 6).toUpperCase(),
    cliente: cliente.trim(),
    producto: producto.trim(),
    cantidad: numCantidad,
    nota: (nota || '').trim(),
    creado_en: new Date().toISOString()
  };

  try {
    // TODO 3: Publicar pedido en topic 'pedidos' usando como clave el nombre del cliente (R6)
    const recordMetadata = await producer.send({
      topic: 'pedidos',
      messages: [
        {
          key: pedido.cliente,
          value: JSON.stringify(pedido)
        }
      ]
    });

    // TODO 4: Retornar comprobante con partición y offset reales confirmados por Kafka (R1)
    const meta = recordMetadata && recordMetadata[0] ? recordMetadata[0] : null;
    if (!meta) {
      throw new Error('Kafka no retornó metadatos de confirmación de publicación');
    }

    const particionReal = meta.partition;
    const offsetReal = Number(meta.baseOffset !== undefined ? meta.baseOffset : (meta.offset ?? 0));

    console.log(`📤 [Tienda -> Kafka] Pedido #${pedido.id} publicado en P${particionReal}@${offsetReal} (Clave: "${pedido.cliente}")`);

    return res.status(200).json({
      resultados: [
        {
          pedido,
          particion: particionReal,
          offset: offsetReal
        }
      ]
    });
  } catch (errSend) {
    console.error('❌ Error al publicar comanda en Kafka:', errSend);
    return res.status(500).json({
      ok: false,
      error: 'Falla al persistir comanda en Kafka',
      mensaje: errSend.message
    });
  }
});

// ============================================================================
// INICIO DEL SERVIDOR Y CONEXIÓN KAFKAJS
// ============================================================================
// Función de orquestación de arranque con reconexión continua
async function iniciarSistemaKafka() {
  let iniciado = false;
  while (!iniciado) {
    try {
      await asegurarInfraestructuraKafka();
      await inicializarProductor();
      await inicializarConsumidor();
      iniciado = true;
      console.log('🚀 Sistema de Cafetería KafkaJS completamente operativo.');
    } catch (errInit) {
      console.error('⚠️ Falla al iniciar componentes KafkaJS. Reintentando en 3s...', errInit.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

const server = app.listen(PORT, () => {
  console.log(`☕ Servidor de Cafetería Kafka escuchando en el puerto ${PORT}`);
  console.log(`   - Tienda: http://localhost:${PORT}`);
  console.log(`   - Cocina: http://localhost:${PORT}/cocina`);
  console.log(`   - Health: http://localhost:${PORT}/api/health`);
  console.log(`   - Brokers Kafka objetivo: ${kafkaBrokers.join(', ')}`);

  iniciarSistemaKafka().catch((err) => {
    console.error('❌ Error no recuperable en iniciarSistemaKafka:', err);
  });
});

// Cierre ordenado y desconexión limpia de KafkaJS
async function cerrarServidor(signal) {
  console.log(`Recibido ${signal}, cerrando servidor HTTP y conexiones KafkaJS...`);
  try {
    if (consumer) {
      console.log('Desconectando Consumidor Kafka...');
      await consumer.disconnect();
    }
    if (producer) {
      console.log('Desconectando Productor Kafka...');
      await producer.disconnect();
    }
  } catch (e) {
    console.error('Error al desconectar KafkaJS:', e.message);
  }
  server.close(() => {
    console.log('Servidor HTTP cerrado.');
    process.exit(0);
  });
}

process.on('SIGTERM', () => cerrarServidor('SIGTERM'));
process.on('SIGINT', () => cerrarServidor('SIGINT'));

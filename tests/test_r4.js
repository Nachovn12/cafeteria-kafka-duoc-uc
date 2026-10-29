/**
 * test_r4.js - Suite automatizada de verificación para el requisito R4 (Escalamiento Horizontal: Dos Cocinas)
 * Cafetería Kafka — Duoc UC
 *
 * Valida la distribución equitativa de particiones del topic 'pedidos' entre dos instancias
 * del consumidor pertenecientes al mismo consumer group ('grupo-cocina').
 */

const http = require('http');

const COCINA_1_URL = process.env.COCINA_1_URL || 'http://localhost:8081';
const COCINA_2_URL = process.env.COCINA_2_URL || 'http://localhost:8083';

// Los 20 pedidos de prueba diseñados para distribuir entre las 3 particiones
const PEDIDOS_PRUEBA = [
  { id: 'R4-001', cliente: 'Ana', producto: 'Café Espresso', cantidad: 1, nota: 'R4 Prueba 01' },
  { id: 'R4-002', cliente: 'Fernando', producto: 'Café Latte', cantidad: 2, nota: 'R4 Prueba 02' },
  { id: 'R4-003', cliente: 'Carlos', producto: 'Medialuna de Manteca', cantidad: 3, nota: 'R4 Prueba 03' },
  { id: 'R4-004', cliente: 'David', producto: 'Café Americano', cantidad: 2, nota: 'R4 Prueba 04' },
  { id: 'R4-005', cliente: 'Gloria', producto: 'Muffin de Arándanos', cantidad: 1, nota: 'R4 Prueba 05' },
  { id: 'R4-006', cliente: 'Jorge', producto: 'Café Americano', cantidad: 1, nota: 'R4 Prueba 06' },
  { id: 'R4-007', cliente: 'Elena', producto: 'Cappuccino', cantidad: 1, nota: 'R4 Prueba 07' },
  { id: 'R4-008', cliente: 'Hugo', producto: 'Sandwich Jamón y Queso', cantidad: 1, nota: 'R4 Prueba 08' },
  { id: 'R4-009', cliente: 'Rosa', producto: 'Café Espresso', cantidad: 2, nota: 'R4 Prueba 09' },
  { id: 'R4-010', cliente: 'Isabel', producto: 'Café Latte', cantidad: 2, nota: 'R4 Prueba 10' },
  { id: 'R4-011', cliente: 'Karla', producto: 'Café Espresso', cantidad: 1, nota: 'R4 Prueba 11' },
  { id: 'R4-012', cliente: 'Tomas', producto: 'Café Latte', cantidad: 1, nota: 'R4 Prueba 12' },
  { id: 'R4-013', cliente: 'Luis', producto: 'Mocaccino', cantidad: 1, nota: 'R4 Prueba 13' },
  { id: 'R4-014', cliente: 'Nicolas', producto: 'Cappuccino', cantidad: 2, nota: 'R4 Prueba 14' },
  { id: 'R4-015', cliente: 'Ursula', producto: 'Muffin de Arándanos', cantidad: 2, nota: 'R4 Prueba 15' },
  { id: 'R4-016', cliente: 'Maria', producto: 'Tostón con Palta', cantidad: 1, nota: 'R4 Prueba 16' },
  { id: 'R4-017', cliente: 'Olga', producto: 'Tostón con Palta', cantidad: 2, nota: 'R4 Prueba 17' },
  { id: 'R4-018', cliente: 'Valeria', producto: 'Sandwich Jamón y Queso', cantidad: 1, nota: 'R4 Prueba 18' },
  { id: 'R4-019', cliente: 'Pedro', producto: 'Medialuna de Manteca', cantidad: 2, nota: 'R4 Prueba 19' },
  { id: 'R4-020', cliente: 'Sergio', producto: 'Café Americano', cantidad: 1, nota: 'R4 Prueba 20' }
];

const eventosCocina1 = [];
const eventosCocina2 = [];

/**
 * Conecta a un endpoint SSE y recolecta eventos en un arreglo
 */
function conectarSSE(urlBase, listaEventos, nombreCocina) {
  return new Promise((resolve, reject) => {
    const sseUrl = `${urlBase}/api/eventos`;
    console.log(`🔌 Conectando SSE a ${nombreCocina} (${sseUrl})...`);

    const req = http.get(sseUrl, (res) => {
      if (res.statusCode !== 200) {
        return reject(new Error(`Error conectando a ${nombreCocina}: HTTP ${res.statusCode}`));
      }

      let buffer = '';
      let conectado = false;

      res.on('data', (chunk) => {
        buffer += chunk.toString('utf-8');
        const lines = buffer.split('\n\n');
        buffer = lines.pop(); // guardar fragmento incompleto

        for (const line of lines) {
          const trimmed = line.trim();
          if (trimmed.startsWith('data:')) {
            try {
              const data = JSON.parse(trimmed.replace(/^data:\s*/, ''));
              if (data.tipo === 'conexion' && !conectado) {
                conectado = true;
                console.log(`✅ Handshake SSE completado con ${nombreCocina}.`);
                resolve(req);
              } else if (data.pedido && data.pedido.id) {
                listaEventos.push({
                  cocina: nombreCocina,
                  id: data.pedido.id,
                  cliente: data.pedido.cliente,
                  producto: data.pedido.producto,
                  particion: data.particion,
                  offset: data.offset,
                  historico: data.historico,
                  timestamp: data.timestamp
                });
              }
            } catch (e) {
              // Ignorar pings o fragmentos no parseables
            }
          }
        }
      });

      res.on('error', (err) => {
        console.error(`❌ Error en stream SSE de ${nombreCocina}:`, err.message);
      });
    });

    req.on('error', (err) => {
      reject(err);
    });
  });
}

/**
 * Publica un pedido vía HTTP POST a /api/pedidos
 */
function publicarPedido(urlBase, pedido) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(pedido);
    const targetUrl = new URL('/api/pedidos', urlBase);

    const options = {
      hostname: targetUrl.hostname,
      port: targetUrl.port || 80,
      path: targetUrl.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (d) => { body += d; });
      res.on('end', () => {
        if (res.statusCode === 200) {
          try {
            const resp = JSON.parse(body);
            resolve(resp.resultados[0]);
          } catch (e) {
            reject(new Error(`Error parseando respuesta JSON: ${body}`));
          }
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${body}`));
        }
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function ejecutarPruebaR4() {
  console.log('================================================================');
  console.log('🚀 INICIANDO SUITE DE PRUEBA R4: DOS COCINAS');
  console.log('================================================================');

  let sseReq1 = null;
  let sseReq2 = null;

  try {
    // 1. Conectar ambos streams SSE
    sseReq1 = await conectarSSE(COCINA_1_URL, eventosCocina1, 'Cocina 1 (:8081)');
    sseReq2 = await conectarSSE(COCINA_2_URL, eventosCocina2, 'Cocina 2 (:8083)');

    // Esperar 2 segundos para asegurar suscripción activa
    await new Promise(r => setTimeout(r, 2000));

    console.log('\n📦 Publicando lote de 20 pedidos de prueba (R4-001 a R4-020)...');
    const resultadosPublicacion = [];

    for (const pedido of PEDIDOS_PRUEBA) {
      const res = await publicarPedido(COCINA_1_URL, pedido);
      resultadosPublicacion.push({
        id: pedido.id,
        cliente: pedido.cliente,
        producto: pedido.producto,
        particion: res.particion,
        offset: res.offset
      });
      console.log(`   [POST OK] #${pedido.id} | Cliente: ${pedido.cliente.padEnd(10)} -> P${res.particion} @ Offset ${res.offset}`);
      // Pequeño intervalo de 100ms para simular tráfico realista
      await new Promise(r => setTimeout(r, 100));
    }

    // Esperar 4 segundos a que todos los mensajes sean consumidos y retransmitidos vía SSE
    console.log('\n⏳ Esperando recepción y procesamiento en ambas cocinas...');
    await new Promise(r => setTimeout(r, 4000));

    // Analizar resultados
    console.log('\n================================================================');
    console.log('📊 RESULTADOS DE RECEPCIÓN EN COCINAS KDS');
    console.log('================================================================');

    const idsCocina1 = eventosCocina1.filter(e => e.id.startsWith('R4-')).map(e => e.id);
    const idsCocina2 = eventosCocina2.filter(e => e.id.startsWith('R4-')).map(e => e.id);

    console.log(`\n🔹 Total de pedidos R4 recibidos en Cocina 1 (:8081): ${idsCocina1.length}`);
    console.log(`   IDs: ${idsCocina1.join(', ')}`);

    console.log(`\n🔹 Total de pedidos R4 recibidos en Cocina 2 (:8083): ${idsCocina2.length}`);
    console.log(`   IDs: ${idsCocina2.join(', ')}`);

    // Detectar duplicados (intersección)
    const duplicados = idsCocina1.filter(id => idsCocina2.includes(id));
    console.log(`\n🔍 Verificación de Duplicados en ambas cocinas:`);
    if (duplicados.length === 0) {
      console.log('   ✅ NINGÚN pedido apareció simultáneamente en ambas cocinas. Duplicados: 0.');
    } else {
      console.log(`   ❌ ALERTA: Pedidos duplicados encontrados: ${duplicados.join(', ')}`);
    }

    // Detectar faltantes
    const todosRecibidos = new Set([...idsCocina1, ...idsCocina2]);
    const faltantes = PEDIDOS_PRUEBA.filter(p => !todosRecibidos.has(p.id)).map(p => p.id);
    console.log(`\n🔍 Verificación de Faltantes (de 20 esperados):`);
    if (faltantes.length === 0) {
      console.log(`   ✅ Los 20 pedidos fueron recibidos exitosamente (Total: ${todosRecibidos.size}/20).`);
    } else {
      console.log(`   ❌ ALERTA: Faltan pedidos: ${faltantes.join(', ')}`);
    }

    // Tabla detallada por pedido
    console.log('\n📋 TABLA DETALLADA DE DISTRIBUCIÓN POR PEDIDO:');
    console.log('ID      | Cliente    | Producto                | Partición | Offset | Recibido Por');
    console.log('--------+------------+-------------------------+-----------+--------+----------------');
    for (const p of resultadosPublicacion) {
      const enC1 = idsCocina1.includes(p.id);
      const enC2 = idsCocina2.includes(p.id);
      let destino = 'NINGUNA';
      if (enC1 && enC2) destino = 'AMBAS (DUPLICADO!)';
      else if (enC1) destino = 'Cocina 1 (:8081)';
      else if (enC2) destino = 'Cocina 2 (:8083)';

      console.log(`${p.id.padEnd(7)} | ${p.cliente.padEnd(10)} | ${p.producto.padEnd(23)} | P${p.particion}         | ${String(p.offset).padEnd(6)} | ${destino}`);
    }

    console.log('\n================================================================');
    if (duplicados.length === 0 && faltantes.length === 0 && todosRecibidos.size === 20) {
      console.log('🎉 REQUISITO R4 CUMPLIDO EXITOSAMENTE');
    } else {
      console.log('❌ REQUISITO R4 NO CUMPLIDO');
    }
    console.log('================================================================\n');

  } catch (err) {
    console.error('❌ Error ejecutando suite R4:', err);
  } finally {
    if (sseReq1) sseReq1.destroy();
    if (sseReq2) sseReq2.destroy();
  }
}

// Exportar función o ejecutar directamente si es llamado por CLI
if (require.main === module) {
  ejecutarPruebaR4();
}

module.exports = { ejecutarPruebaR4, PEDIDOS_PRUEBA };


/**
 * tienda.js - Lógica del Punto de Venta (Tienda)
 * Cafetería Kafka — Duoc UC
 * 
 * Cumple con principios de interface-design, frontend-design y better-ui
 */

document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('form-pedido');
  const btnEnviar = document.getElementById('btn-enviar');
  const btnText = document.getElementById('btn-text');
  const bannerEstado = document.getElementById('banner-estado');
  const comprobanteArea = document.getElementById('comprobante-area');
  const inputCliente = document.getElementById('cliente');
  const selectProducto = document.getElementById('producto');
  const inputCantidad = document.getElementById('cantidad');
  const inputNota = document.getElementById('nota');

  const btnQtyMinus = document.getElementById('btn-qty-minus');
  const btnQtyPlus = document.getElementById('btn-qty-plus');
  const productPresets = document.querySelectorAll('.preset-chip');
  const quickNotePills = document.querySelectorAll('.quick-note-pill');

  // ==========================================================================
  // MICROINTERACCIONES & CONTROLES POS
  // ==========================================================================

  // Sincronizar Fichas Rápidas de Producto
  productPresets.forEach((chip) => {
    chip.addEventListener('click', () => {
      const productName = chip.getAttribute('data-product');
      selectProducto.value = productName;
      actualizarChipsProducto(productName);
      selectProducto.dispatchEvent(new Event('change'));
    });
  });

  selectProducto.addEventListener('change', () => {
    actualizarChipsProducto(selectProducto.value);
  });

  function actualizarChipsProducto(selectedName) {
    productPresets.forEach((chip) => {
      if (chip.getAttribute('data-product') === selectedName) {
        chip.classList.add('selected');
      } else {
        chip.classList.remove('selected');
      }
    });
  }

  // Stepper de Cantidad (+ / -)
  if (btnQtyMinus && btnQtyPlus && inputCantidad) {
    btnQtyMinus.addEventListener('click', () => {
      let val = parseInt(inputCantidad.value, 10) || 1;
      if (val > 1) {
        inputCantidad.value = val - 1;
      }
    });

    btnQtyPlus.addEventListener('click', () => {
      let val = parseInt(inputCantidad.value, 10) || 1;
      if (val < 20) {
        inputCantidad.value = val + 1;
      }
    });
  }

  // Notas Rápidas Presets
  quickNotePills.forEach((pill) => {
    pill.addEventListener('click', () => {
      const noteText = pill.getAttribute('data-note');
      const current = inputNota.value.trim();
      if (!current) {
        inputNota.value = noteText;
      } else if (!current.toLowerCase().includes(noteText.toLowerCase())) {
        inputNota.value = `${current}, ${noteText}`;
      }
      inputNota.focus();
    });
  });

  // ==========================================================================
  // ENVÍO DE PEDIDO A LA API
  // ==========================================================================

  form.addEventListener('submit', async (e) => {
    e.preventDefault();

    const cliente = inputCliente.value.trim();
    const producto = selectProducto.value;
    const cantidad = parseInt(inputCantidad.value, 10);
    const nota = inputNota.value.trim();

    if (!cliente) {
      mostrarBanner('error', 'Nombre de cliente requerido', 'Por favor ingresa el nombre de la persona que solicita el pedido.');
      inputCliente.focus();
      return;
    }

    if (!producto) {
      mostrarBanner('error', 'Producto no seleccionado', 'Selecciona una bebida o alimento del catálogo de cafetería.');
      selectProducto.focus();
      return;
    }

    if (isNaN(cantidad) || cantidad <= 0) {
      mostrarBanner('error', 'Cantidad inválida', 'La cantidad mínima por orden debe ser al menos 1.');
      inputCantidad.focus();
      return;
    }

    const payload = {
      cliente,
      producto,
      cantidad,
      nota
    };

    setLoading(true);
    ocultarBanner();

    try {
      const response = await fetch('/api/pedidos', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      const data = await response.json();

      if (response.ok && data.resultados && data.resultados.length > 0) {
        // Pedido confirmado por Kafka (Requisito R1)
        const resultado = data.resultados[0];
        mostrarBanner(
          'info',
          'Pedido Confirmado en Kafka',
          `Registrado en partición ${resultado.particion}, offset ${resultado.offset}. Despachado hacia la cocina.`
        );
        renderizarComprobanteExitoso(resultado);
        form.reset();
        inputCantidad.value = '1';
        actualizarChipsProducto('');
        inputCliente.focus();
      } else if (response.status === 501) {
        // Estado esperado mientras KafkaJS no esté implementado (TODO 3 y 4)
        mostrarBanner(
          'warning',
          'Publicación Kafka Pendiente (TODO 3 y 4)',
          data.mensaje || 'El pedido ha sido validado por el servidor pero el Productor KafkaJS aún no está activo. No se generan datos ficticios.'
        );
        renderizarEstadoPendiente(payload, data);
      } else {
        mostrarBanner(
          'error',
          'Error al procesar comanda',
          data.mensaje || data.error || `Código de respuesta HTTP ${response.status}`
        );
      }
    } catch (err) {
      console.error('Error de red al despachar pedido:', err);
      mostrarBanner(
        'error',
        'Falla de Conexión con Backend',
        'No fue posible comunicar con el servidor en /api/pedidos. Verifica que el proceso Node.js esté activo.'
      );
    } finally {
      setLoading(false);
    }
  });

  // ==========================================================================
  // RENDERIZADO VISUAL DEL COMPROBANTE (better-ui & interface-design)
  // ==========================================================================

  function setLoading(isLoading) {
    if (isLoading) {
      btnEnviar.disabled = true;
      btnEnviar.innerHTML = `
        <span class="icon" data-lucide="refresh-cw" data-size="18"></span>
        <span>Publicando en Kafka...</span>
      `;
    } else {
      btnEnviar.disabled = false;
      btnEnviar.innerHTML = `
        <span class="icon" data-lucide="send" data-size="18"></span>
        <span id="btn-text">Confirmar y Despachar a Kafka</span>
      `;
    }
    if (window.LucideIcons) window.LucideIcons.render();
  }

  function mostrarBanner(tipo, titulo, mensaje) {
    const iconName = tipo === 'info' ? 'check-circle' : tipo === 'warning' ? 'alert-triangle' : 'alert-circle';
    const iconSvg = window.LucideIcons ? window.LucideIcons.get(iconName, { size: 18 }) : '';

    bannerEstado.className = `status-banner banner-${tipo}`;
    bannerEstado.innerHTML = `
      <div class="icon-col">${iconSvg}</div>
      <div class="text-col">
        <div class="status-banner-title">${titulo}</div>
        <div>${mensaje}</div>
      </div>
    `;
    bannerEstado.style.display = 'flex';
  }

  function ocultarBanner() {
    bannerEstado.style.display = 'none';
  }

  function renderizarComprobanteExitoso(resultado) {
    const pedido = resultado.pedido;
    const hora = pedido.creado_en ? new Date(pedido.creado_en).toLocaleTimeString() : new Date().toLocaleTimeString();
    const iconReceipt = window.LucideIcons ? window.LucideIcons.get('receipt', { size: 16 }) : '';
    const iconClock = window.LucideIcons ? window.LucideIcons.get('clock', { size: 14 }) : '';
    const iconLayers = window.LucideIcons ? window.LucideIcons.get('layers', { size: 14 }) : '';

    comprobanteArea.innerHTML = `
      <div class="ticket-header-meta">
        <div class="ticket-shop-name">Cafetería Kafka &bull; Ticket Oficial</div>
        <div class="ticket-order-tag">#${pedido.id || 'N/A'}</div>
        <div class="ticket-timestamp">
          ${iconClock}
          <span class="tabular-nums">${hora}</span>
        </div>
      </div>

      <div class="ticket-customer-badge">
        <span>Cliente:</span>
        <strong>${pedido.cliente}</strong>
      </div>

      <div class="ticket-item-row">
        <span class="ticket-item-name">${pedido.producto}</span>
        <span class="ticket-item-qty tabular-nums">${pedido.cantidad}x</span>
      </div>

      ${pedido.nota ? `
      <div class="ticket-note-box">
        Nota: "${pedido.nota}"
      </div>` : ''}

      <div class="kafka-ledger-box">
        <div class="kafka-ledger-head">
          <span>${iconLayers} Registro Confirmado en Kafka</span>
          <span>ACK 1</span>
        </div>
        <div class="kafka-metrics-grid">
          <div class="kafka-metric-card">
            <div class="kafka-metric-val tabular-nums">${resultado.particion}</div>
            <div class="kafka-metric-lbl">Partición</div>
          </div>
          <div class="kafka-metric-card">
            <div class="kafka-metric-val tabular-nums">${resultado.offset}</div>
            <div class="kafka-metric-lbl">Offset</div>
          </div>
        </div>
      </div>
    `;
  }

  function renderizarEstadoPendiente(pedido, data) {
    const iconAlert = window.LucideIcons ? window.LucideIcons.get('alert-triangle', { size: 16 }) : '';
    const iconLayers = window.LucideIcons ? window.LucideIcons.get('layers', { size: 14 }) : '';

    comprobanteArea.innerHTML = `
      <div class="ticket-header-meta">
        <div class="ticket-shop-name">Comanda en Espera de Publicación</div>
        <div class="ticket-order-tag" style="color: var(--status-warning);">#STAGED</div>
        <div class="ticket-timestamp">Validada localmente por el servidor</div>
      </div>

      <div class="ticket-customer-badge">
        <span>Cliente:</span>
        <strong>${pedido.cliente}</strong>
      </div>

      <div class="ticket-item-row">
        <span class="ticket-item-name">${pedido.producto}</span>
        <span class="ticket-item-qty tabular-nums">${pedido.cantidad}x</span>
      </div>

      ${pedido.nota ? `
      <div class="ticket-note-box">
        Nota: "${pedido.nota}"
      </div>` : ''}

      <div class="status-banner banner-warning" style="margin-top: 1.25rem; margin-bottom: 0;">
        <div class="icon-col">${iconAlert}</div>
        <div class="text-col">
          <div class="status-banner-title">Productor KafkaJS no conectado</div>
          <div style="font-size: 0.8rem; margin-top: 0.2rem;">
            ${data.mensaje || 'TODO 2, 3 y 4 pendientes en server.js. No se devuelven particiones ni offsets ficticios.'}
          </div>
        </div>
      </div>
    `;
  }
});

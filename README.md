# Registros Cámaras para Home Assistant

Integración de **Registros Cámaras 2**, mantenida en
[Siroguh/registros_camaras_home_assistant](https://github.com/Siroguh/registros_camaras_home_assistant).
Usa la API autenticada de la aplicación nueva; no consulta directamente D1.

## Instalación y configuración

Requiere Home Assistant **2026.10.3 o posterior** y un servidor que implemente
`GET /api/integration/:centreId`, `GET /api/environment/streams/:centreId` y
`POST /api/environment/updates`, versión de contrato 1, con confirmación durable
final `ReadingTransaction`.

1. Añade este repositorio como integración personalizada en HACS, o en la
   Marketplace integrada de HA 2026.11 o posterior.
2. Instala y reinicia Home Assistant.
3. En **Ajustes → Dispositivos y servicios → Añadir integración**, busca Registros Cámaras.
4. Introduce la URL raíz de la aplicación, el centro (por defecto `villarroya-01-01`)
    y una **clave API nativa de Better Auth**, creada desde **Cuenta** en la aplicación.

Para instalación manual, extrae el ZIP de la release en el directorio de configuración
de HA: debe quedar `custom_components/registros_camaras/manifest.json`. Reinicia HA
y añade la integración siguiendo los pasos 3 y 4. La configuración actual usa
entradas nativas de versión 2; reautenticación cambia la clave y reconfiguración
cambia la URL conservando el centro.

La URL debe ser accesible desde HA. Para desarrollo puede ser
`https://omarchythinkpad.powan-bee.ts.net:1301` mientras `vp run dev` esté activo.
No uses `localhost` para apuntar a otro ordenador. Se valida TLS y no se siguen
redirecciones de las peticiones autenticadas.
La integración nunca entrega la clave a las tarjetas ni a los diagnósticos.

No se ha enviado este repositorio al catálogo público: se instala como repositorio
personalizado. `hacs.json` sirve para HACS y Marketplace; no es una app/add-on de HA OS.

## Envío de actualizaciones de sensores

La integración escucha cada reporte de los sensores asignados por el servidor,
incluidos los que repiten exactamente el valor y los atributos. La URL interna o
externa de este HA debe coincidir con `source` de esas asociaciones. Los eventos
se copian inmediatamente y se agrupan sin perder muestras (hasta 100 por lote de
la misma fuente y asociación, ventana de dos segundos). Los reportes iguales conservan identidad propia incluso
si comparten timestamp. Cada `sourceEventId` identifica un reporte; el `commandId`
identifica su lote de transporte y se conserva junto al payload en cada reintento.

Una cola con `homeassistant.helpers.storage.Store`, en la clave versionada
`registros_camaras.updates.<entry_id>`, conserva muestras y lotes hasta recibir
confirmación durable del servidor. Usa escritura atómica nativa HA. Reintentos usan
el mismo UUID y contenido; un reinicio o una respuesta HTTP perdida no duplica la
lectura. El I/O y la serialización se ejecutan fuera del event loop. La durabilidad
comienza al verificar el guardado: un apagado abrupto anterior puede perder lo todavía en RAM.
No borrar la cola para resolver una incidencia de conexión.
El índice usa la versión de Store 2. Los segmentos separan fuente y asociación,
igual que los lotes de envío:
sus ACK avanzan el prefijo sin reescribir las muestras de otras asociaciones.
La regresión intercalada de 36 asociaciones y 3600 reportes no reescribe segmentos
al drenar la cola.
Las muestras se guardan en segmentos inmutables de hasta 1000 reportes; los
metadatos confirman segmentos, cursores y ACK juntos. Drenar 50 000 reportes en la
regresión escribe unos 17,2 MB, con commits de hasta 34 KB, sin reescribir la cola
completa por lote, incluso con una caché de 1500 tareas. Las cachés se escriben solo
cuando cambian, en archivos separados referenciados por el índice. Los segmentos
y cachés usan un subdirectorio para acotar las claves retenidas por HA. La recogida
de huérfanos requiere un índice válido: si falta o está dañado, se conservan los
archivos y se señala la necesidad de recuperación, sin iniciar una cola vacía.
El apagado se coordina antes de que HA cancele las tareas, espera el executor y
conserva el respaldo `FINAL_WRITE` con escritura ordenada. Cancela el transporte
remoto sin esperar su timeout; el vaciado local sobrevive al plazo del job de HA.
Si una descarga falla al guardar, el siguiente apagado vuelve a intentar el vaciado
pendiente en vez de reutilizar la tarea fallida.

La cola queda vinculada al servidor y centro, sin guardar la clave API. Una rotación
de clave conserva el destino y reanuda el lote. Reconfigurar otra URL o centro
bloquea la entrega de la cola anterior y conserva sus datos; volver al destino
original permite reanudarla. Con la cola vacía, cambiar de destino reinicia los
cursores y las cachés anteriores. Para un destino independiente se usa otra entrada.
Las últimas asociaciones verificadas se conservan para ese destino y se escuchan
antes de esperar a HTTP durante un reinicio sin red. El dashboard conserva su
última proyección como no disponible hasta recuperar conexión. Un manifiesto que
retira todas las asociaciones locales retira también sus listeners.
Si solo falta el archivo de caché, el índice, lote y cursores válidos se conservan
y se vuelve a consultar el servidor; sin red ni manifiesto verificado no se captura.
La caché del dashboard sigue los sondeos verificados: escribe como máximo una vez
por minuto durante ejecución y vacía la última proyección al descargar/apagar.
Solo esta caché reconstruible se coalesce, nunca las muestras de sensores. Un fallo
de caché se diagnostica y reintenta sin ocultar el recibo de un comando aceptado.
Si falla la primera escritura y su limpieza durante setup, HA mantiene el
reintento automático de configuración. Las entradas ya cargadas conservan su
vaciado completo durante descarga/apagado.

Recorder reconcilia huecos cada cinco minutos desde `historyFrom` de cada asociación
o su último cursor persistido. El backfill inicial respeta ese instante del servidor,
incluso cuando precede al inicio de la captura live. Fija cada ventana, espera la sincronización del escritor
Recorder con una barrera encolada (también con `commit_interval=60` y un evento
desencolado aún en proceso); un timeout conserva el cursor. Excluye estados
sintéticos. Los cambios coincidentes usan tiempo, huella del estado textual y ordinal;
los atributos que Recorder puede descartar no intervienen en esa identidad y
la recuperación reconstruye esas identidades sin coalescer cambios repetidos.
La conversión comprimida nativa conserva `null` para una baja de entidad y `""`
para un estado vacío genuino: solo se excluye el primero. `unknown`, `unavailable`
y cadenas vacías reales se conservan como reportes inválidos/no disponibles.
Las consultas se limitan a 1001 filas (1000 más una de avance), usando los
constructores nativos de Recorder en `history.py`, su sesión y su conversión.
La API pública de historia de HA 2026.10.3 no ofrece límite de filas y la alternativa
con `limit` omite cambios de atributos: se aíslan aquí los helpers internos de HA,
sin SQL textual, conexión propia ni esquema paralelo. Requieren pruebas con Recorder
real al actualizar HA. Las páginas y las adaptaciones al espacio libre terminan
entre milisegundos completos, sin dividir/reidentificar ocurrencias coincidentes.
Los límites SQL se calculan con el mismo redondeo float→datetime que la captura
live; una fila justo antes del límite que redondea hacia él pasa a la página
siguiente sin perderse ni cambiar de identidad.
Si un único milisegundo supera 1000 filas, se conserva su cursor pendiente y se
diagnostica el límite; las demás asociaciones siguen recuperándose. No se afirma
que ese grupo excesivo se haya recuperado. El orden de atención rota entre pasadas.
Cuando Recorder descarta todos los atributos por tamaño, la coincidencia conserva
la unidad y el payload live. Un histórico todavía sin unidad permanece almacenado
sin bloquear otros reportes ni congelar un lote incompleto; la captura coincidente
puede completarlo antes del envío. Los lotes ya preparados conservan su payload.
Estos históricos incompletos tienen un cupo separado de 1000 reportes: no consumen
la capacidad de captura/envío live. Si se llena, la recuperación conserva su cursor
y queda pendiente; captura y entrega continúan aunque nunca aparezca la unidad.
Los datos recuperados son históricos y nunca renuevan su tiempo ni activan alertas
antiguas. Recorder no conserva necesariamente reportes idénticos: no puede recuperar
todos los reportes durante una pausa de captura, ni datos purgados o excluidos.

Los diagnósticos incluyen cola, buffer RAM, capacidad, pausa y errores seguros.
El buffer tiene límite de 10 000 y Store de 50 000 muestras (más una reserva de
10 000 para vaciar RAM al descargar/apagar); saturación pausa captura
y conserva lo ya recibido. Al drenar la cola se reanuda. Errores de credenciales,
contrato o asociación conservan el lote rechazado para corregir su causa.
El ACK final de ingestión exige `commandId` idéntico al UUID enviado, stream exacto
`ReadingTransaction/<UUID>`, versión de tipo entero entre 4 y 103 y
`eventId: ReadingTransaction/<UUID>:<version>`. HA valida ese contrato sin replicar
el agrupamiento de familias de Domain.
Booleans, otros streams/UUID, versiones fuera de contrato o eventId incoherentes
no confirman la entrega; el mismo lote permanece pendiente de reintento.
Si queda capacidad parcial, el writer reduce el prefijo pendiente hasta que cabe,
confirma su persistencia, retira solo ese prefijo de RAM y despierta al sender.
No espera a que quepa todo el bloque de RAM para aprovechar los huecos disponibles.
Un ACK durable despierta al writer que espera capacidad; los errores reales de
almacenamiento conservan su backoff. Un bloqueo definitivo no libera capacidad.
Los rechazos **definitivos demostrados por Domain** usan este contrato específico
de `POST /api/environment/updates` (HTTP **422**):

```json
{"_tag":"SensorUpdatesRejected","apiVersion":1,"commandId":"UUID-del-lote","reason":"assignment_window","accepted":false}
```

El servidor solo puede emitirlo antes de aceptar ese comando: un fallo de disco,
timeout, conflicto de identidad, reloj futuro corregible o ACK perdido no lo prueban.
La integración verifica versión, UUID, razón allowlisted y `accepted: false`. Una
respuesta HTTP genérica conserva el reintento original; requiere el servidor con
este contrato para aislar automáticamente el rechazo de una asociación cerrada.

El lote demostrado como rechazado se conserva íntegro en un Store inmutable con
su UUID/payload y razón; **no es un ACK**, no se descartan ni reasignan sus lecturas.
Las otras asociaciones pendientes pueden avanzar: cada lote pertenece a una sola
fuente y asociación.
Los bloqueados comparten el presupuesto total de 50 000 muestras más la reserva
de apagado de 10 000; no son almacenamiento ilimitado ni se borran para liberar
capacidad. Al agotar ese total se pausa la captura, aunque continúa el envío de
filas sanas ya guardadas. No hay una acción de borrado/reasignación automática.
Los diagnósticos muestran número de lotes/reportes bloqueados, razón pública y
presupuesto compartido; no exponen UUID, muestras, URL ni secretos. La resolución
de los bloqueados requiere un procedimiento explícito que conserve su evidencia.

La cadencia de sensores y sus relojes deben ser compatibles con los 30 segundos de
frescura live del servidor. Conservar todos los reportes genera almacenamiento
proporcional a su frecuencia; agruparlos reduce peticiones, no muestras.

## Entidades del dashboard

Un dispositivo de centro contiene conexión, tareas pendientes/vencidas y actualizar.
Cada cámara tiene:

- Cantidad y número de lotes (con ocupantes en atributos).
- Estado del plan: vacía, sin plan/fase, compatible o conflicto.
- Mínimo/máximo de temperatura y humedad **efectivos**, no lecturas del sensor.
- Propuesta y sus límites/ID; propuesta pendiente/vencida, conflicto y manual activa.

Los sensores físicos de temperatura/humedad siguen siendo los de HA. Sondeo compartido
cada 30 segundos. Las altas de cámaras se descubren sin reiniciar. Si falla la conexión,
la cámara está archivada o el servidor está reconciliando, las entidades operativas
están `unavailable`, nunca representan cero existencias por un fallo de lectura.
Los atributos `centre_id`, `location_id`, `revision` y `active_lots` permiten construir
tarjetas y conservar los indicadores visuales de cámaras ocupadas.

## Acciones nativas

Las llamadas de usuario requieren administrador de Home Assistant.
Las automatizaciones internas pueden utilizar las mismas acciones.

Todas están bajo `registros_camaras`, tienen campos en el editor de acciones y
admiten `return_response`/`response_variable`:

| Acción | Uso |
| --- | --- |
| `get_snapshot` | Estado versionado, catálogo, cámaras y tareas pendientes. |
| `get_lots` | Página de inventario; continuar con `nextBefore` como `before`. |
| `get_lot` | Ficha; comprobar `status: projected`. |
| `get_movement_options` | Disponibilidad al día efectivo; `correcting` opcional. |
| `get_movement_history` | Operaciones; comprobar `status: projected`. |
| `confirm_proposal` | Confirmar exactamente `proposal_id` y revisión mostrados. |
| `schedule_manual`, `replace_manual`, `finish_manual` | Ciclo auditado de consignas manuales. |
| `complete_task` | Resultado tipado y `task_revision`; retirada no mueve stock. |
| `create_lot` | Alta con código, cantidad y día local. |
| `create_movement`, `correct_movement` | Repartos, mezcla, procedencias y motivo de corrección. |

Siempre se selecciona `config_entry_id`. Los comandos exigen un UUID `command_id`
y `expected_revision` del centro. Guarda ambos junto con el contenido: reintentar
la misma operación conserva el UUID, revisión y datos, incluso tras reiniciar HA.
Nunca se reintenta automáticamente una escritura con otra revisión. Un conflicto
requiere consultar y revisar los datos; un fallo de transporte puede significar que
ya fue aceptada. Una lectura fallida después de la aceptación no oculta su recibo.
Los recibos de negocio requieren `commandId`, `eventId`, `stream` y una `version`
entera no negativa, vinculados al comando enviado. El identificador del hecho de
negocio puede ser independiente del stream y la versión.

## Tarjetas

Se pueden usar tarjetas nativas Entity/Entities/Tile/History con estas entidades.
Los nombres de entidad los asigna HA; selecciónalos mediante el editor visual.

Las tarjetas personalizadas
[registros_camaras_cards](https://github.com/Siroguh/registros_camaras_cards) **1.0+**
usan estas mismas acciones nativas. Conservan cámara/consignas, alta de lotes,
movimientos totales/parciales, procedencias, corrección, diálogos y editores.

Para una tarjeta propia:

```js
const { response: snapshot } = await hass.callWS({
  type: "call_service", domain: "registros_camaras", service: "get_snapshot",
  service_data: { config_entry_id: entryId }, return_response: true
});
// Mostrar snapshot; conservar su revisión y el ID que el usuario revisó.
// Crear una vez con crypto.randomUUID(), y conservar para reintentos.
await hass.callWS({
  type: "call_service", domain: "registros_camaras", service: "confirm_proposal",
  service_data: { config_entry_id: entryId, command_id: commandId,
    expected_revision: snapshot.revision, location_id: locationId, proposal_id: proposalId },
  return_response: true
});
```

## Plantilla y desarrollo

Estructura basada en la plantilla recomendada por HACS
[custom-components/blueprint](https://github.com/custom-components/blueprint), commit
`cd399dac2a7e88d899dd62b3f5d8bbf91f1adadd`: config flow, cliente asíncrono,
`DataUpdateCoordinator`, `CoordinatorEntity`, `ConfigEntry.runtime_data` y descarga.
Se adaptó a las APIs actuales de HA y al servidor. Licencia/atribución en `NOTICE`.

Referencias consultadas el 27/11/2026:
- https://www.hacs.xyz/docs/publish/integration/
- https://contributors.home-assistant.io/marketplace/guides/integrations/
- https://contributors.home-assistant.io/marketplace/reference/hacs-json/

```sh
uv sync --locked
sh scripts/check.sh
```

Pruebas con Home Assistant real 2026.10.3, HTTP local, flujos, reautenticación,
entidades, descubrimiento, disponibilidad, servicios, descarga y recarga.
El plugin de fixtures se fija por separado y su dependencia HA se ajusta a la
versión mínima con `override-dependencies`. No instala dependencias de tests en HA.
`package.py` genera `dist/registros_camaras.zip` sin credenciales ni cachés.

Desde el servidor, la prueba cruzada opcional ejecuta el cliente Python contra un
Worker/D1/R2 desechable y comprueba Bearer, snapshot y rechazo de revisión obsoleta:

```sh
REGISTROS_HA_CHECKOUT=/ruta/registros_camaras_home_assistant \
  vp test run scripts/observations.test.ts -t 'real Worker persists operational tasks'
```

# Informe TP Nivelador

## 1. Introducción

El sistema implementa un servidor de lotería que recibe apuestas de múltiples agencias, las persiste, y una vez que todas las agencias terminaron de enviar sus apuestas, calcula y devuelve a cada una la lista de sus propios ganadores. El servidor está escrito en Python y el cliente en Go. A continuación se detallan el protocolo de comunicación diseñado y los mecanismos utilizados para sincronizar la ejecución concurrente del servidor.

## 2. Protocolo de comunicación

Se implementó un protocolo sobre TCP, con frames con longitud prefijada, para evitar los problemas de short read/short write.

### 2.1 Formato de mensaje

Cada mensaje se compone de:

| Campo             | Tamaño   | Descripción                                   |
|-------------------|----------|------------------------------------------------|
| Longitud| 4 bytes (big-endian) | Cantidad de bytes que ocupan tipo + payload |
| Tipo de mensaje    | 1 byte   | Identifica el tipo de mensaje (ver tabla)      |
| Payload            | variable | Datos serializados como texto plano, con `\n` y `\|` como separadores |

El largo se calcula como tipo + payload, y ese valor es lo primero que se lee del socket para saber cuántos bytes adicionales hay que leer a continuación. Tanto el envío como la recepción se resuelven con funciones auxiliares que reintentan hasta completar la cantidad exacta de bytes pedida, cubriendo los casos de I/O parcial.

Un punto de diseño que resultó importante a la hora del desarrollo fue que cada mensaje se arma como un único buffer contiguo (longitud + tipo + payload) y se escribe con una sola llamada al socket, en lugar de hacer una escritura para el prefijo de longitud y otra para el cuerpo. Esto evita que, especialmente del lado del cliente, un mensaje lógico termine fragmentado en dos segmentos TCP separados, lo cual degradaría innecesariamente la relación entre el tamaño de batch configurado y el tamaño real de los paquetes en la red.

### 2.2 Tipos de mensaje

| Tipo         | Valor | Dirección        | Payload                                                        |
|--------------|-------|-------------------|------------------------------------------------------------------|
| `FINISHED`   | 0x02  | cliente → servidor | id de agencia                                                    |
| `WINNERS`    | 0x03  | servidor → cliente | lista de ganadores de esa agencia, una apuesta por línea         |
| `BATCH`      | 0x04  | cliente → servidor | id de agencia + N apuestas, una por línea  |
| `BATCH_ACK`  | 0x05  | servidor → cliente | 1 byte: 1 si el batch se persistió correctamente, 0 si no |

### 2.3 Flujo de comunicación

1. El cliente abre la conexión y envía sus apuestas en **batches** de tamaño configurable (`BATCH_SIZE`), agrupando varias apuestas por mensaje en lugar de enviarlas de a una. Esto reduce drásticamente la cantidad de mensajes (y por lo tanto de round-trips y overhead de protocolo) para un mismo volumen de datos.
2. Por cada batch enviado, el cliente espera de forma síncrona el `BATCH_ACK` antes de continuar con el siguiente, garantizando que el servidor pudo persistir el batch anterior.
3. Cuando terminó de leer su archivo de entrada, el cliente envía `FINISHED` y queda esperando bloqueado la respuesta `WINNERS`.
4. El servidor, al recibir `FINISHED` de una agencia, la marca como terminada y espera (si hace falta) a que el resto de las agencias necesarias para el quórum también terminen, antes de calcular y devolver los ganadores correspondientes a **esa** agencia.

## 3. Sincronización de la ejecución concurrente

El servidor atiende a cada agencia en un **thread independiente** (`ClientHandler`, subclase de `threading.Thread`), lo que permite recibir y procesar batches de distintas agencias en paralelo. Esto introduce dos problemas de concurrencia que se resuelven de estas maneras:

### 3.1 Acceso concurrente al almacenamiento

Todas las apuestas se persisten en un único archivo CSV compartido (`bets.csv`). Como varios threads pueden querer escribir (`store_bets`) o leer (`load_bets`) al mismo tiempo, el acceso al storage está protegido por un `threading.Lock()` (`storage_lock`), tomado con `with self.storage_lock:` tanto al guardar un batch como al recorrer el archivo completo para calcular los ganadores de una agencia. Esto serializa el acceso al recurso compartido y evita condiciones de carrera / corrupción del archivo, a costa de que las escrituras de distintas agencias no se solapen entre sí (aceptable dado que la operación de I/O es rápida en relación al resto del procesamiento).

### 3.2 Sincronización del quórum (sorteo)

El sorteo no puede realizarse hasta que **todas** las agencias esperadas (`AGENCY_QUORUM_MIN`) hayan enviado `FINISHED`. Para coordinar esto entre threads se usa una **variable de condición** (`threading.Condition`), junto con:

- `finished_agencies`: un `set()` compartido con los IDs de las agencias que ya terminaron.
- `draw_done`: una flag compartida que indica si ya se alcanzó el quórum.

Cuando un thread recibe `FINISHED`:
1. Toma el lock interno de la condición (`with self.quorum_condition:`).
2. Agrega su `agency_id` a `finished_agencies`.
3. Si con esa agencia se alcanzó el quórum (`len(finished_agencies) >= quorum_min`), marca `draw_done[0] = True` y hace `notify_all()`, despertando a todos los threads que estaban esperando.
4. Si todavía no se alcanzó el quórum, el thread se bloquea en `wait_for(...)`, liberando el lock mientras espera, hasta que otro thread cumpla la condición o el servidor se esté apagando.

Este patrón evita tanto *busy-waiting* como condiciones de carrera al leer/escribir el estado compartido del quórum, ya que toda lectura y escritura de `finished_agencies` y `draw_done` ocurre bajo el mismo lock.

### 3.3 Apagado ordenado (graceful shutdown)

El servidor captura `SIGTERM`/`SIGINT` en el thread principal. Al recibir la señal:
- Se limpia un `threading.Event()` (`running_event`) que todos los threads consultan como propiedad `running`.
- Se cierra el socket de escucha, para que el loop de `accept()` termine.
- Se notifica a la `quorum_condition`, para despertar a cualquier `ClientHandler` que estuviera bloqueado esperando el quórum, y así pueda salir limpiamente en lugar de quedar colgado indefinidamente.

Finalmente, el thread principal hace `join()` (con timeout) sobre todos los threads de clientes activos antes de terminar, asegurando que no queden threads huérfanos ni conexiones a medio procesar.

## 4. Conclusión

A medida que avancé con el TP me di cuenta que lo más dificil no es diseñar un protocolo para una comunicación confiable en un mismo lenguaje, sino que la dificultad se encuentra en hacer que dos programas, hechos en distintos lenguajes, puedan comunicarse de forma confiable. 
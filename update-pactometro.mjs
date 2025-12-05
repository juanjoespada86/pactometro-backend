// update-pactometro.mjs
// Lee los datos de la Junta y actualiza Supabase para el pactómetro

import { createClient } from '@supabase/supabase-js';

// 1) Variables de entorno que nos pasará GitHub Actions
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  JE_HOST,
  JE_USER,
  JE_PASS,
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY (revisa los Secrets de GitHub).');
  process.exit(1);
}

if (!JE_HOST || !JE_USER || !JE_PASS) {
  console.error('Faltan JE_HOST, JE_USER o JE_PASS (revisa los Secrets de GitHub).');
  process.exit(1);
}

// 2) Cliente de Supabase (lado servidor, con service_role)
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// 3) Helper para llamadas a la Junta con Basic Auth
async function fetchFromJunta(path) {
  const url = `${JE_HOST}${path}`;

  const authHeader = 'Basic ' + Buffer
    .from(`${JE_USER}:${JE_PASS}`)
    .toString('base64');

  const res = await fetch(url, {
    headers: {
      Authorization: authHeader,
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Error HTTP ${res.status} al llamar ${url}: ${text}`);
  }

  return res.text();
}

// 4) Obtener el numEnv actual
async function getCurrentNumEnv() {
  // url: /descargas/csv/data/getEnvio/510 (sin parámetros) :contentReference[oaicite:2]{index=2}
  const csv = await fetchFromJunta('/descargas/csv/data/getEnvio/510');

  // Según la doc, devuelve registros de texto plano separados por ";" :contentReference[oaicite:3]{index=3}
  const line = csv.trim().split('\n')[0];
  const parts = line.split(';');

  // ⚠️ Aquí la doc NO detalla el diseño de campos de getEnvio.
  // Vamos a asumir que el número de envío está en parts[1].
  // Si al probar ves que no cuadra, bastará con cambiar el índice (1, 2, etc.)
  const numEnv = parts[1];

  if (!numEnv) {
    throw new Error(`No se ha podido obtener numEnv a partir de la línea: ${line}`);
  }

  return numEnv;
}

// 5) Obtener la línea "CM" (Comunidad Autónoma) del fichero de totales
async function getTotalesLineaCM(numEnv) {
  // url: /descargas/csv/data/getEscrutinioTotales/510/{numEnv} :contentReference[oaicite:4]{index=4}
  const csv = await fetchFromJunta(`/descargas/csv/data/getEscrutinioTotales/510/${numEnv}`);

  const lines = csv.trim().split('\n');

  // Estructura del fichero de totales: el segundo campo es el identificador de registro CM/PR :contentReference[oaicite:5]{index=5}
  const lineaCM = lines.find(line => {
    const parts = line.split(';');
    return parts[1] === 'CM';
  });

  if (!lineaCM) {
    throw new Error('No se ha encontrado ninguna línea con identificador "CM" en el CSV de totales.');
  }

  return lineaCM;
}

// 6) Parsear la línea CM y extraer candidaturas
function parseCandidaturasFromLineaCM(lineaCM) {
  // Según el diseño de "ESCRUTINIO DE TOTALES": :contentReference[oaicite:6]{index=6}
  //  - 22 campos de cabecera (fecha, identificador, códigos, nombre, mesas, censo, votos, etc.)
  //  - Luego tabla de 50 candidaturas, cada una con 5 campos:
  //    código (4), siglas (55), votos (8), porcentaje (5), diputados electos (3) :contentReference[oaicite:7]{index=7}

  const fields = lineaCM.split(';');

  const NUM_HEADER_FIELDS = 22;
  const candidaturaFields = fields.slice(NUM_HEADER_FIELDS);

  const candidaturas = [];

  for (let i = 0; i + 4 < candidaturaFields.length; i += 5) {
    const codigo = candidaturaFields[i]?.trim();
    const siglasRaw = candidaturaFields[i + 1] ?? '';
    const votosRaw = candidaturaFields[i + 2] ?? '';
    const pctRaw = candidaturaFields[i + 3] ?? '';
    const escañosRaw = candidaturaFields[i + 4] ?? '';

    // Saltamos candidaturas vacías
    if (!codigo || codigo === '0000') continue;

    const siglas = siglasRaw.trim();
    if (!siglas) continue;

    const votos = votosRaw ? Number(votosRaw) : 0;

    // En campos porcentaje las dos últimas cifras son decimales (ej. "03782" = 37,82%) :contentReference[oaicite:8]{index=8}
    const porcentaje = pctRaw ? Number(pctRaw) / 100 : null;

    const escaños = escañosRaw ? Number(escañosRaw) : 0;

    // Generamos un party_id simple a partir de las siglas
    const partyId = siglas
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_]/g, '');

    candidaturas.push({
      party_id: partyId,
      party_name: siglas,
      seats_2025: escaños,
      vote_pct_2025: porcentaje,
      votos_totales: votos,
      codigo_candidatura: codigo,
    });
  }

  return candidaturas;
}

// 7) Upsert en Supabase (creando partidos nuevos y manteniendo seats_2023 de los antiguos)
async function upsertCandidaturasEnSupabase(candidaturas) {
  if (candidaturas.length === 0) {
    console.log('No hay candidaturas que upsertar.');
    return;
  }

  // Primero leemos qué partidos existen ya y con cuántos escaños 2023
  const { data: existentes, error: errorExistentes } = await supabase
    .from('pactometro_results')
    .select('party_id, seats_2023');

  if (errorExistentes) {
    throw errorExistentes;
  }

  const mapaExistentes = new Map(
    (existentes || []).map(row => [row.party_id, row.seats_2023])
  );

  // Preparamos filas para upsert:
  // - Si el partido ya existe -> mantenemos seats_2023
  // - Si es nuevo -> seats_2023 = 0
  const rows = candidaturas.map(c => {
    const seats2023 = mapaExistentes.has(c.party_id)
      ? mapaExistentes.get(c.party_id)
      : 0;

    return {
      party_id: c.party_id,           // UNIQUE o PK en la tabla
      party_name: c.party_name,
      seats_2025: c.seats_2025,
      vote_pct_2025: c.vote_pct_2025,
      seats_2023: seats2023,
      updated_at: new Date().toISOString(),
    };
  });

  // Upsert por party_id
  const { error } = await supabase
    .from('pactometro_results')
    .upsert(rows, { onConflict: 'party_id' });

  if (error) {
    throw error;
  }
}

// 8) Función principal
async function main() {
  try {
    console.log('--- Actualizando pactómetro ---');

    const numEnv = await getCurrentNumEnv();
    console.log('Número de envío actual:', numEnv);

    const lineaCM = await getTotalesLineaCM(numEnv);
    console.log('Línea CM obtenida (inicio):', lineaCM.slice(0, 120) + '...');

    const candidaturas = parseCandidaturasFromLineaCM(lineaCM);
    console.log('Candidaturas parseadas:', candidaturas);

    await upsertCandidaturasEnSupabase(candidaturas);

    console.log('✅ Actualización completada correctamente.');
  } catch (err) {
    console.error('❌ Error durante la actualización:', err);
    process.exit(1);
  }
}

// Ejecutar
main();

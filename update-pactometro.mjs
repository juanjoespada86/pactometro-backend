// update-pactometro.mjs
// Lee los datos de la Junta y actualiza Supabase para el pactómetro
// - Resultados autonómicos (tabla pactometro_results)
// - Resultados por provincia (tabla pactometro_province_results)

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
  // /descargas/csv/data/getEnvio/510
  const csv = await fetchFromJunta('/descargas/csv/data/getEnvio/510');

  // Nos quedamos con la primera línea, por si acaso hubiera más
  const line = csv.trim().split('\n')[0];

  const parts = line.split(';');

  let numEnv;
  if (parts.length === 1) {
    // Caso actual: "51"
    numEnv = parts[0].trim();
  } else {
    // Caso posible: "20251221;51;..."
    numEnv = (parts[1] || '').trim();
  }

  if (!numEnv) {
    throw new Error(`No se ha podido obtener numEnv a partir de la línea: ${line}`);
  }

  console.log('numEnv detectado desde getEnvio:', numEnv);
  return numEnv;
}

// 5) Obtener la línea CM (Extremadura) y las líneas PR (provincias) del fichero de totales
async function getTotalesData(numEnv) {
  // url: /descargas/csv/data/getEscrutinioTotales/510/{numEnv}
  const csv = await fetchFromJunta(`/descargas/csv/data/getEscrutinioTotales/510/${numEnv}`);
  const lines = csv.trim().split('\n');

  let lineaCM = null;
  const provincias = [];

  for (const line of lines) {
    const parts = line.split(';');
    const tipo = (parts[1] || '').trim(); // "CM" o "PR"

    if (tipo === 'CM') {
      lineaCM = line;
    } else if (tipo === 'PR') {
      // En estas líneas viene la provincia (Badajoz / Cáceres)
      const provinceName = (parts[5] || '').trim(); // ej. "Badajoz", "Cáceres"

      // Generamos un ID de provincia estable a partir del nombre (sin acentos, minúsculas, con guiones bajos)
      const provinceId = provinceName
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '') // quitar acentos
        .toLowerCase()
        .replace(/\s+/g, '_'); // espacios -> guión bajo

      provincias.push({
        province_id: provinceId,
        province_name: provinceName,
        linea: line,
      });
    }
  }

  if (!lineaCM) {
    throw new Error('No se ha encontrado ninguna línea con identificador "CM" en el CSV de totales.');
  }

  console.log('Línea CM obtenida (inicio):', lineaCM.slice(0, 120) + '...');
  console.log('Provincias detectadas:', provincias.map(p => p.province_name));

  return { lineaCM, provincias };
}

// 6) Parsear candidaturas (CM o PR) y extraer candidaturas
function parseCandidaturasFromLinea(linea) {
  const fields = linea.split(';');

  const NUM_HEADER_FIELDS = 22; // hasta "Número de votos faltantes"
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

    // Nombre que queremos usar en el pactómetro
    let displayName = siglas;

    // 🟣 Regla especial: "PODEMOS-IU-AV" => "Unidas por Extremadura"
    if (siglas === 'PODEMOS-IU-AV') {
      displayName = 'Unidas por Extremadura';
    }

    const votos = votosRaw ? Number(votosRaw) : 0;
    const porcentaje = pctRaw ? Number(pctRaw) / 100 : null; // p.ej "544" -> 5.44
    const escaños = escañosRaw ? Number(escañosRaw) : 0;

    const partyId = siglas
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_]/g, '');

    candidaturas.push({
      party_id: partyId,         // seguirá siendo "podemosiuav"
      party_name: displayName,   // "Unidas por Extremadura"
      seats_2025: escaños,
      vote_pct_2025: porcentaje,
      votos_totales: votos,
      codigo_candidatura: codigo,
    });
  }

  return candidaturas;
}

// Compatibilidad: por si en algún momento usas el nombre antiguo
function parseCandidaturasFromLineaCM(lineaCM) {
  return parseCandidaturasFromLinea(lineaCM);
}

// 6bis) Parsear porcentaje de censo escrutado de la línea de totales (CM o PR)
function parsePctEscrutadoFromLinea(linea) {
  const fields = linea.split(';');

  // Según documentación, el campo "Porcentaje de censo escrutado"
  // es el décimo campo del fichero de totales.
  const raw = (fields[9] || '').trim(); // índice 9 (0-based)

  if (!raw) {
    return null;
  }

  const num = Number(raw);
  if (Number.isNaN(num)) {
    console.warn('No se puede parsear pct_escrutado a partir de:', raw);
    return null;
  }

  // Los porcentajes vienen con las dos últimas posiciones como decimales,
  // es decir "0544" => 5.44 %
  return num / 100;
}

// 7) Upsert autonómico en Supabase (lo que ya teníamos) + pct_escrutado
async function upsertCandidaturasEnSupabase(candidaturas, pctEscrutado) {
  if (candidaturas.length === 0) {
    console.log('No hay candidaturas autonómicas que upsertar.');
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
  const nowIso = new Date().toISOString();

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
      pct_escrutado: pctEscrutado,    // 👈 nuevo campo
      updated_at: nowIso,
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

// 8) Nuevo: upsert por provincias en Supabase
async function upsertCandidaturasProvinciaEnSupabase(provinciasConCandidaturas) {
  const rows = [];
  const nowIso = new Date().toISOString();

  provinciasConCandidaturas.forEach(prov => {
    const { province_id, province_name, candidaturas } = prov;

    (candidaturas || []).forEach(c => {
      rows.push({
        province_id,
        province_name,
        party_id: c.party_id,
        party_name: c.party_name,
        seats_2025: c.seats_2025,
        vote_pct_2025: c.vote_pct_2025,
        votos_totales: c.votos_totales,
        updated_at: nowIso,
        // Si en el futuro añades pct_escrutado a esta tabla,
        // aquí podríamos añadirlo también.
      });
    });
  });

  if (rows.length === 0) {
    console.log('No hay candidaturas provinciales que upsertar.');
    return;
  }

  const { error } = await supabase
    .from('pactometro_province_results')
    .upsert(rows, { onConflict: 'province_id,party_id' });

  if (error) {
    throw error;
  }
}

// 9) Función principal
async function main() {
  try {
    console.log('--- Actualizando pactómetro ---');

    const numEnv = await getCurrentNumEnv();
    console.log('Número de envío actual:', numEnv);

    // Obtenemos línea CM (Extremadura) y PR (provincias)
    const { lineaCM, provincias } = await getTotalesData(numEnv);

    // Porcentaje de censo escrutado a nivel comunidad
    const pctEscrutadoCM = parsePctEscrutadoFromLinea(lineaCM);
    console.log('Porcentaje de censo escrutado CM:', pctEscrutadoCM);

    // Candidaturas autonómicas
    const candidaturasCM = parseCandidaturasFromLinea(lineaCM);
    console.log('Candidaturas autonómicas parseadas:', candidaturasCM);

    await upsertCandidaturasEnSupabase(candidaturasCM, pctEscrutadoCM);

    // Candidaturas provinciales
    const provinciasConCandidaturas = provincias.map(p => ({
      ...p,
      candidaturas: parseCandidaturasFromLinea(p.linea),
    }));

    console.log(
      'Candidaturas provinciales parseadas (resumen):',
      provinciasConCandidaturas.map(p => ({
        province: p.province_name,
        parties: p.candidaturas.map(c => ({
          id: c.party_id,
          seats_2025: c.seats_2025,
        })),
      }))
    );

    await upsertCandidaturasProvinciaEnSupabase(provinciasConCandidaturas);

    console.log('✅ Actualización completada correctamente.');
  } catch (err) {
    console.error('❌ Error durante la actualización:', err);
    process.exit(1);
  }
}

// Ejecutar
main();

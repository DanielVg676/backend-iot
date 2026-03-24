import { dbPool, query } from "../../config/db";
import { Collar } from "../../types/collar.types";
import { Animal } from "../../types/animal.types";

export interface CreateCollarInput {
  collarId: string;
  tenantId?: string | null;
  firmwareVersion?: string | null;
  purchasedAt?: string | null;
}

export async function insertCollar(input: CreateCollarInput): Promise<Collar> {
  const sql = `
    INSERT INTO collars (
      collar_id,
      tenant_id,
      status,
      firmware_version,
      purchased_at
    ) VALUES ($1, $2, $3, $4, $5)
    RETURNING
      id,
      collar_id,
      tenant_id,
      animal_id,
      status,
      firmware_version,
      linked_at,
      purchased_at
  `;

  const params = [
    input.collarId,
    input.tenantId ?? null,
    "inactive", // estado inicial por defecto
    input.firmwareVersion ?? null,
    input.purchasedAt ?? null,
  ];

  const { rows } = await query<Collar>(sql, params);
  return rows[0];
}

export async function getCollarById(id: string, tenantId?: string): Promise<Collar | null> {
  const params: any[] = [id];
  const conditions: string[] = ["id = $1"]; // PK

  if (tenantId) {
    params.push(tenantId);
    conditions.push(`tenant_id = $${params.length}`);
  }

  const whereClause = `WHERE ${conditions.join(" AND ")}`;
  const sql = `
    SELECT
      id,
      collar_id,
      tenant_id,
      animal_id,
      status,
      firmware_version,
      linked_at,
      purchased_at
    FROM collars
    ${whereClause}
    LIMIT 1
  `;

  const { rows } = await query<Collar>(sql, params);
  return rows[0] || null;
}

export async function getCollarByCollarId(collarId: string, tenantId?: string): Promise<Collar | null> {
  const params: any[] = [collarId];
  const conditions: string[] = ["collar_id = $1"]; // identificador IoT

  if (tenantId) {
    params.push(tenantId);
    conditions.push(`tenant_id = $${params.length}`);
  }

  const whereClause = `WHERE ${conditions.join(" AND ")}`;
  const sql = `
    SELECT
      id,
      collar_id,
      tenant_id,
      animal_id,
      status,
      firmware_version,
      linked_at,
      purchased_at
    FROM collars
    ${whereClause}
    LIMIT 1
  `;

  const { rows } = await query<Collar>(sql, params);
  return rows[0] || null;
}

export async function getAnimalById(animalId: string, tenantId?: string): Promise<Animal | null> {
  const params: any[] = [animalId];
  const conditions: string[] = ["id = $1"]; // asumiendo PK id

  if (tenantId) {
    params.push(tenantId);
    conditions.push(`tenant_id = $${params.length}`);
  }

  const whereClause = `WHERE ${conditions.join(" AND ")}`;
  const sql = `
    SELECT
      id,
      tenant_id,
      upp_id,
      siniiga_tag,
      sex,
      birth_date,
      status
    FROM animals
    ${whereClause}
    LIMIT 1
  `;

  const { rows } = await query<Animal>(sql, params);
  return rows[0] || null;
}

export async function linkCollarToAnimalTx(params: {
  collarId: string;
  animalId: string;
  tenantId?: string;
  linkedBy?: string;
}): Promise<void> {
  const { collarId, animalId, tenantId, linkedBy } = params;

  if (!dbPool) {
    throw new Error("DB no configurada");
  }

  const client = await dbPool.connect();

  try {
    await client.query("BEGIN");

    // Actualizar collar
    const updateParams: any[] = [animalId, new Date().toISOString(), collarId];
    let updateCondition = "WHERE collar_id = $3";

    if (tenantId) {
      updateParams.push(tenantId);
      updateCondition += ` AND tenant_id = $${updateParams.length}`;
    }

    await client.query(
      `UPDATE collars SET animal_id = $1, linked_at = $2, status = 'linked' ${updateCondition}`,
      updateParams
    );

    // Insertar en historial
    const historySql = `
      INSERT INTO collar_animal_history (
        collar_id_fk,
        animal_id,
        linked_at,
        linked_by
      ) VALUES ($1, $2, $3, $4)
    `;
    const historyParams = [collarId, animalId, new Date().toISOString(), linkedBy ?? null];

    await client.query(historySql, historyParams);

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function unlinkCollarTx(params: {
  collarId: string;
  tenantId?: string;
  unlinkedBy?: string;
}): Promise<void> {
  const { collarId, tenantId, unlinkedBy } = params;

  if (!dbPool) {
    throw new Error("DB no configurada");
  }

  const client = await dbPool.connect();

  try {
    await client.query("BEGIN");

    // Obtener animal asociado actual
    const currentSql = `
      SELECT animal_id FROM collars
      WHERE collar_id = $1
      ${tenantId ? "AND tenant_id = $2" : ""}
      LIMIT 1
    `;
    const currentParams = tenantId ? [collarId, tenantId] : [collarId];
    const currentRes = await client.query<{ animal_id: string | null }>(currentSql, currentParams);
    const currentAnimalId = currentRes.rows[0]?.animal_id;

    // Actualizar collar
    const updateParams: any[] = [null, "unlinked", collarId];
    let updateCondition = "WHERE collar_id = $3";

    if (tenantId) {
      updateParams.push(tenantId);
      updateCondition += ` AND tenant_id = $${updateParams.length}`;
    }

    await client.query(
      `UPDATE collars SET animal_id = $1, status = $2 ${updateCondition}`,
      updateParams
    );

    // Insertar en historial
    const historySql = `
      INSERT INTO collar_animal_history (
        collar_id_fk,
        animal_id,
        unlinked_at,
        unlinked_by
      ) VALUES ($1, $2, $3, $4)
    `;
    const historyParams = [
      collarId,
      currentAnimalId,
      new Date().toISOString(),
      unlinkedBy ?? null,
    ];

    await client.query(historySql, historyParams);

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function updateCollarTenant(
  collarUuid: string,
  tenantId: string | null
): Promise<Collar | null> {
  const sql = `
    UPDATE collars
    SET tenant_id = $2,
        updated_at = now()
    WHERE id = $1
    RETURNING
      id,
      collar_id,
      tenant_id,
      animal_id,
      status,
      firmware_version,
      linked_at,
      purchased_at
  `;

  const params = [collarUuid, tenantId];
  const { rows } = await query<Collar>(sql, params);
  return rows[0] || null;
}

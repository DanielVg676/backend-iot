import { Router } from "express";
import {
  createCollarHandler,
  assignCollar,
  getCollarAssignmentHandler,
  unassignCollarHandler,
  assignCollarTenantHandler,
  unassignCollarTenantHandler,
} from "../controllers/collar.controller";

const router = Router();

// Crear un nuevo collar
router.post("/collars", createCollarHandler);

// Asignar / desasignar tenant al collar (por UUID)
router.post("/collars/:collarId/tenant/assign", assignCollarTenantHandler);
router.post("/collars/:collarId/tenant/unassign", unassignCollarTenantHandler);

// Asignar collar a animal
router.post("/collars/:collarId/assign", assignCollar);

// Desasignar collar de animal
router.post("/collars/:collarId/unassign", unassignCollarHandler);

// Consultar asignación actual de un collar
router.get("/collars/:collarId/assignment", getCollarAssignmentHandler);

export default router;

"""Custom Work Order hooks."""

from __future__ import annotations

from collections.abc import Iterable

import frappe
from frappe.utils import cint

from erpnext.manufacturing.doctype.work_order.work_order import (
    create_job_card,
)


def on_submit(doc, _method: str | None = None) -> None:
    """Auto-create job cards for every workstation/shift combination on submit."""
    if not doc.custom_plant_name:
        return

    shifts = _get_shifts()
    if not shifts:
        return

    workstations = _get_workstations(doc.custom_plant_name)
    if not workstations:
        return

    operations = list(doc.get("operations") or [])
    if not operations:
        return

    existing = {
        (row.operation_id, row.workstation, row.custom_shift_number)
        for row in frappe.get_all(
            "Job Card",
            filters={"work_order": doc.name},
            fields=["operation_id", "workstation", "custom_shift_number"],
        )
    }

    scrap_by_bom: dict[str, list[frappe._dict]] = {}

    for op in operations:
        for workstation in workstations:
            for shift in shifts:
                key = (op.name, workstation.name, shift.name)
                if key in existing:
                    continue

                bom_no = op.bom or doc.bom_no

                row = frappe._dict(
                    operation=op.operation,
                    workstation=workstation.name,
                    workstation_type=workstation.workstation_type,
                    custom_shift_number=shift.name,
                    name=op.name,
                    bom=bom_no,
                    sequence_id=op.sequence_id,
                    batch_size=getattr(op, "batch_size", None),
                    job_card_qty=0,
                    wip_warehouse=getattr(op, "wip_warehouse", None),
                    source_warehouse=getattr(op, "source_warehouse", None),
                    hour_rate=getattr(op, "hour_rate", None),
                    pending_qty=doc.qty,
                )

                job_card = create_job_card(doc, row, auto_create=True)

                scrap_rows = scrap_by_bom.get(bom_no)
                if scrap_rows is None:
                    scrap_rows = _get_bom_scrap_items(bom_no)
                    scrap_by_bom[bom_no] = scrap_rows

                if scrap_rows:
                    _apply_scrap_items(job_card, scrap_rows)

                existing.add(key)


def _get_shifts() -> Iterable[frappe._dict]:
    return frappe.get_all("Shift", fields=["name"], order_by="name asc")


def _get_workstations(plant_name: str) -> Iterable[frappe._dict]:
    return frappe.get_all(
        "Workstation",
        filters={"plant_floor": plant_name},
        fields=["name", "workstation_type"],
        order_by="name asc",
    )


def _get_bom_scrap_items(bom_no: str | None) -> list[frappe._dict]:
    if not bom_no:
        return []

    return frappe.db.sql(
        """
        SELECT item_code, item_name, stock_qty, stock_uom
        FROM `tabBOM Scrap Item`
        WHERE parent = %(bom)s AND parenttype = 'BOM' AND parentfield = 'scrap_items'
        ORDER BY idx
        """,
        {"bom": bom_no},
        as_dict=True,
    )


def _apply_scrap_items(job_card, scrap_rows: list[frappe._dict]) -> None:
    if not job_card or not scrap_rows:
        return

    job_card.set("scrap_items", [])

    for row in scrap_rows:
        job_card.append(
            "scrap_items",
            {
                "item_code": row.item_code,
                "item_name": row.item_name,
                "stock_qty": row.stock_qty,
                "stock_uom": row.stock_uom,
                "qty": row.stock_qty,
            },
        )

    job_card.flags.ignore_validate_update_after_submit = True
    job_card.save(ignore_permissions=True)

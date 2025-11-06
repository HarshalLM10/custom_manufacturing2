from __future__ import annotations

from collections.abc import Iterable

import frappe
from frappe import _


def execute(filters: dict | None = None):
    filters = frappe._dict(filters or {})
    columns = get_columns()
    data = get_data(filters)
    return columns, data


def get_columns() -> list[dict]:
    return [
        {"label": _("Workstation"), "fieldname": "workstation", "fieldtype": "Link", "options": "Workstation", "width": 180},
        {"label": _("Plant"), "fieldname": "plant_floor", "fieldtype": "Link", "options": "Plant Floor", "width": 140},
        {"label": _("Qty Before Replacement"), "fieldname": "threshold_qty", "fieldtype": "Float", "width": 170},
        {"label": _("Completed Qty"), "fieldname": "completed_qty", "fieldtype": "Float", "width": 140},
        {"label": _("Remaining Qty"), "fieldname": "remaining_qty", "fieldtype": "Float", "width": 140},
        {"label": _("Status"), "fieldname": "status", "fieldtype": "Data", "width": 180},
    ]


def get_data(filters: frappe._dict) -> list[dict]:
    conditions: list[str] = []
    values: dict[str, object] = {}

    if filters.plant:
        conditions.append("plant_floor = %(plant)s")
        values["plant"] = filters.plant

    where_clause = f"WHERE {' and '.join(conditions)}" if conditions else ""

    rows = frappe.db.sql(
        f"""
        SELECT
            name,
            plant_floor,
            IFNULL(custom_working_hours_before_replacement, 0) AS threshold_qty,
            IFNULL(custom_worked_hours, 0) AS completed_qty
        FROM `tabWorkstation`
        {where_clause}
        ORDER BY plant_floor, name
        """,
        values,
        as_dict=True,
    )

    data: list[dict] = []
    for row in rows:
        threshold_qty = float(row.threshold_qty or 0)
        completed_qty = float(row.completed_qty or 0)

        remaining_qty = threshold_qty - completed_qty if threshold_qty else 0.0
        status = _("Within Limit")
        if threshold_qty and completed_qty >= threshold_qty:
            status = _("Parts Replacement Required")

        data.append(
            {
                "workstation": row.name,
                "plant_floor": row.plant_floor,
                "threshold_qty": threshold_qty,
                "completed_qty": completed_qty,
                "remaining_qty": remaining_qty,
                "status": status,
            }
        )

    return data

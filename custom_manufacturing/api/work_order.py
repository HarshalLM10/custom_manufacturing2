from __future__ import annotations

import frappe
from frappe.utils import flt


@frappe.whitelist()
def get_total_manufactured_qty(work_order: str | None = None) -> float:
    """Return total manufactured quantity recorded across Job Cards for the work order."""
    if not work_order:
        return 0.0

    total = frappe.db.sql(
        """
        SELECT COALESCE(SUM(total_completed_qty), 0)
        FROM `tabJob Card`
        WHERE docstatus = 1 AND work_order = %s
        """,
        (work_order,),
    )[0][0]

    return flt(total or 0)

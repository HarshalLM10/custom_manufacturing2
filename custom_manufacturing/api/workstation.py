from __future__ import annotations

import frappe
from frappe import _


@frappe.whitelist()
def reset_worked_hours(workstation: str) -> None:
    """Reset the accumulated completed quantity for the given workstation."""
    if not workstation:
        frappe.throw(_("Workstation is required."))

    if not frappe.db.exists("Workstation", workstation):
        return {"success": False, "message": _("Workstation {0} does not exist.").format(frappe.bold(workstation))}

    frappe.db.set_value("Workstation", workstation, "custom_worked_hours", 0, update_modified=False)
    current_value = frappe.db.get_value("Workstation", workstation, "custom_worked_hours")
    return {"success": True, "worked_hours": current_value or 0}

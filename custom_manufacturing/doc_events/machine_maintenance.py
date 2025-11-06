"""Machine Maintenance hooks."""

from __future__ import annotations

import frappe
from frappe.utils import flt


def on_update(doc, _method: str | None = None) -> None:
    if not doc or not getattr(doc, "machine_name", None):
        return

    maintenance_done = _is_checked(getattr(doc, "maintenance_done", None))

    previous = None
    if not doc.is_new():
        try:
            previous = doc.get_doc_before_save()
        except Exception:
            previous = None

    previously_done = _is_checked(getattr(previous, "maintenance_done", None)) if previous else False

    if maintenance_done and not previously_done:
        _reset_workstation_hours(doc)
    elif not maintenance_done and previously_done:
        _restore_workstation_hours(doc, clear_field=True)


def on_cancel(doc, _method: str | None = None) -> None:
    _restore_workstation_hours(doc, clear_field=True)


def on_trash(doc, _method: str | None = None) -> None:
    _restore_workstation_hours(doc, clear_field=False)


def _reset_workstation_hours(doc) -> None:
    current_hours = flt(
        frappe.db.get_value("Workstation", doc.machine_name, "custom_worked_hours") or 0
    )

    if doc.meta.has_field("custom_previous_worked_hours"):
        if doc.get("custom_previous_worked_hours") in (None, ""):
            try:
                doc.db_set("custom_previous_worked_hours", current_hours, update_modified=False)
            except Exception:
                pass
            doc.custom_previous_worked_hours = current_hours

    frappe.db.set_value("Workstation", doc.machine_name, "custom_worked_hours", 0, update_modified=False)


def _restore_workstation_hours(doc, *, clear_field: bool) -> None:
    if not doc or not getattr(doc, "machine_name", None):
        return

    previous_hours = doc.get("custom_previous_worked_hours")
    if previous_hours in (None, ""):
        try:
            previous_hours = frappe.db.get_value(
                "Machine Maintenance", doc.name, "custom_previous_worked_hours"
            )
        except Exception:
            previous_hours = None

    if previous_hours in (None, ""):
        return

    frappe.db.set_value(
        "Workstation", doc.machine_name, "custom_worked_hours", flt(previous_hours), update_modified=False
    )

    if clear_field and doc.meta.has_field("custom_previous_worked_hours"):
        if not getattr(doc, "flags", None) or not getattr(doc.flags, "in_delete", False):
            try:
                doc.db_set("custom_previous_worked_hours", None, update_modified=False)
            except Exception:
                pass
        doc.custom_previous_worked_hours = None


def _is_checked(value) -> bool:
    if value in (True, 1, "1", "Yes", "yes", "TRUE", "True"):
        return True
    if isinstance(value, str) and value.strip().lower() == "yes":
        return True
    return False

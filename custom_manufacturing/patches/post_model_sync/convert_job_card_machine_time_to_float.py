import frappe


def execute():
    """Move Job Card machine operation time data onto the Float field and drop the legacy Int field."""
    doctype = "Job Card"
    old_field = "custom_total_machine_operation_time"
    new_field = "custom_total_machine_operation_time_float"

    _ensure_new_field(doctype, new_field)
    frappe.reload_doc("manufacturing", "doctype", "job_card")
    _ensure_new_column(doctype, new_field)

    if _column_exists(doctype, old_field):
        frappe.db.sql(
            f"""
            UPDATE `tab{doctype}`
            SET `{new_field}` = IFNULL(`{old_field}`, 0)
            """
        )
        frappe.db.sql_ddl(
            f"""
            ALTER TABLE `tab{doctype}`
            DROP COLUMN `{old_field}`
            """
        )

    old_custom_field = f"{doctype}-{old_field}"
    if frappe.db.exists("Custom Field", old_custom_field):
        frappe.delete_doc("Custom Field", old_custom_field, ignore_permissions=True, force=1)

    frappe.clear_cache(doctype=doctype)


def _ensure_new_field(doctype: str, fieldname: str) -> None:
    custom_field_name = f"{doctype}-{fieldname}"
    if frappe.db.exists("Custom Field", custom_field_name):
        field = frappe.get_doc("Custom Field", custom_field_name)
        updates: dict[str, object] = {}

        if field.fieldtype != "Float":
            updates["fieldtype"] = "Float"

        precision = (field.precision or "").strip()
        if precision != "2":
            updates["precision"] = "2"

        if field.insert_after != "total_time_in_mins":
            updates["insert_after"] = "total_time_in_mins"

        if updates:
            frappe.db.set_value("Custom Field", custom_field_name, updates)
        return

    frappe.get_doc(
        {
            "doctype": "Custom Field",
            "dt": doctype,
            "fieldname": fieldname,
            "label": "Total Machine operation time",
            "fieldtype": "Float",
            "precision": "2",
            "insert_after": "total_time_in_mins",
        }
    ).insert(ignore_permissions=True)


def _ensure_new_column(doctype: str, fieldname: str) -> None:
    if not _column_exists(doctype, fieldname):
        frappe.db.sql_ddl(
            f"""
            ALTER TABLE `tab{doctype}`
            ADD COLUMN `{fieldname}` decimal(21,2) NOT NULL DEFAULT 0
            """
        )
        return

    column_info = frappe.db.sql(
        f"SHOW COLUMNS FROM `tab{doctype}` LIKE %s",
        (fieldname,),
        as_dict=True,
    )

    column_type = (column_info[0].get("Type") or "").lower()
    if "decimal" not in column_type:
        frappe.db.sql_ddl(
            f"""
            ALTER TABLE `tab{doctype}`
            MODIFY COLUMN `{fieldname}` decimal(21,2) NOT NULL DEFAULT 0
            """
        )


def _column_exists(doctype: str, fieldname: str) -> bool:
    return bool(
        frappe.db.sql(
            f"SHOW COLUMNS FROM `tab{doctype}` LIKE %s",
            (fieldname,),
        )
    )

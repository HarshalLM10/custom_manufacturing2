from __future__ import annotations

from collections.abc import Iterable

import frappe
from frappe import _


def execute(filters: dict | None = None):
	"""Build a shift-wise summary of Job Cards within the selected date/plant filters."""
	filters = frappe._dict(filters or {})

	columns = get_columns()
	data = get_data(filters)

	return columns, data


def get_columns() -> list[dict]:
	return [
		{
			"label": _("Items"),
			"fieldname": "metric",
			"fieldtype": "Data",
			"width": 260,
		},
		{
			"label": _("Batch Numbers"),
			"fieldname": "batch_numbers",
			"fieldtype": "Data",
			"width": 200,
		},
		{
			"label": _("Total Qty"),
			"fieldname": "total_qty",
			"fieldtype": "Float",
			"width": 130,
		},
		{
			"label": _("Shift 1 Qty"),
			"fieldname": "shift_1_qty",
			"fieldtype": "Float",
			"width": 120,
		},
		{
			"label": _("Shift 2 Qty"),
			"fieldname": "shift_2_qty",
			"fieldtype": "Float",
			"width": 120,
		},
		{
			"label": _("Shift 3 Qty"),
			"fieldname": "shift_3_qty",
			"fieldtype": "Float",
			"width": 120,
		},
	]


def get_data(filters: frappe._dict) -> list[dict]:
	job_cards = fetch_job_cards(filters)
	if not job_cards:
		return []

	data: list[dict] = []

	item_totals: dict[str, dict[str, float]] = {}
	item_labels: dict[str, str] = {}
	item_work_orders: dict[str, set[str]] = {}
	shift_lookup: dict[str, tuple[str | None, str, str]] = {}

	for jc in job_cards:
		item_code = jc.production_item or _("Unknown Item")
		display_label = jc.item_name or jc.production_item or _("Unknown Item")
		item_labels[item_code] = display_label
		item_work_orders.setdefault(item_code, set()).add(jc.work_order)

		entry = item_totals.setdefault(item_code, {"shift_1_qty": 0.0, "shift_2_qty": 0.0, "shift_3_qty": 0.0})

		shift_key = get_shift_key(jc.custom_shift_number)
		if shift_key:
			entry[shift_key] = entry.get(shift_key, 0.0) + (jc.total_completed_qty or 0.0)

		shift_lookup[jc.name] = (shift_key, item_code, jc.work_order)

	work_order_batches = fetch_work_order_batches({jc.work_order for jc in job_cards})

	item_batches: dict[str, set[str]] = {}
	for item_code, work_orders in item_work_orders.items():
		batch_set: set[str] = set()
		for wo in work_orders:
			batch_set.update(work_order_batches.get(wo, set()))
		if batch_set:
			item_batches[item_code] = batch_set

	scrap_rows = fetch_scrap_items([jc.name for jc in job_cards])
	scrap_totals: dict[str, dict[str, dict[str, float]]] = {}

	for row in scrap_rows:
		shift_key, item_code, work_order = shift_lookup.get(row.parent, (None, None, None))
		if not shift_key or not item_code:
			continue

		item_label = row.item_name or row.item_code or _("Co-Product")
		item_scrap = scrap_totals.setdefault(item_code, {})
		entry = item_scrap.setdefault(item_label, {"shift_1_qty": 0.0, "shift_2_qty": 0.0, "shift_3_qty": 0.0})
		entry[shift_key] = entry.get(shift_key, 0.0) + (row.stock_qty or 0.0)

	for item_code in sorted(item_totals, key=lambda x: item_labels.get(x, x)):
		entry = {
			"metric": item_labels.get(item_code, item_code),
			"shift_1_qty": item_totals[item_code].get("shift_1_qty", 0.0),
			"shift_2_qty": item_totals[item_code].get("shift_2_qty", 0.0),
			"shift_3_qty": item_totals[item_code].get("shift_3_qty", 0.0),
		}
		entry["batch_numbers"] = ", ".join(sorted(item_batches.get(item_code, set())))
		entry["total_qty"] = (
			(entry.get("shift_1_qty") or 0.0)
			+ (entry.get("shift_2_qty") or 0.0)
			+ (entry.get("shift_3_qty") or 0.0)
		)
		data.append(entry)

		for scrap_label in sorted(scrap_totals.get(item_code, {})):
			scrap_entry = {
				"metric": _("Co-Product: {0}").format(scrap_label),
				"shift_1_qty": scrap_totals[item_code][scrap_label].get("shift_1_qty", 0.0),
				"shift_2_qty": scrap_totals[item_code][scrap_label].get("shift_2_qty", 0.0),
				"shift_3_qty": scrap_totals[item_code][scrap_label].get("shift_3_qty", 0.0),
			}
			scrap_entry["batch_numbers"] = ""
			scrap_entry["total_qty"] = (
				(scrap_entry.get("shift_1_qty") or 0.0)
				+ (scrap_entry.get("shift_2_qty") or 0.0)
				+ (scrap_entry.get("shift_3_qty") or 0.0)
			)
			data.append(scrap_entry)

	return data


def fetch_job_cards(filters: frappe._dict) -> Iterable[frappe._dict]:
	conditions = ["docstatus = 1"]
	values: dict[str, object] = {}

	if filters.from_date:
		conditions.append("posting_date >= %(from_date)s")
		values["from_date"] = filters.from_date
	if filters.to_date:
		conditions.append("posting_date <= %(to_date)s")
		values["to_date"] = filters.to_date
	if filters.plant:
		conditions.append("custom_plant_name = %(plant)s")
		values["plant"] = filters.plant

	where_clause = " and ".join(conditions)

	return frappe.db.sql(
		f"""
		SELECT
			name,
			custom_shift_number,
			total_completed_qty,
			production_item,
			item_name,
			work_order
		FROM `tabJob Card`
		WHERE {where_clause}
		""",
		values,
		as_dict=True,
	)


def fetch_scrap_items(job_card_names: list[str]) -> Iterable[frappe._dict]:
	if not job_card_names:
		return []

	return frappe.db.sql(
		"""
		SELECT parent, item_code, item_name, stock_qty
		FROM `tabJob Card Scrap Item`
		WHERE parent IN %(parents)s
		""",
		{"parents": tuple(job_card_names)},
		as_dict=True,
	)


def fetch_work_order_batches(work_orders: set[str]) -> dict[str, set[str]]:
	if not work_orders:
		return {}

	work_orders_tuple = tuple(work_orders)

	rows = frappe.db.sql(
		"""
		SELECT DISTINCT se.work_order, sed.batch_no
		FROM `tabStock Entry` se
		INNER JOIN `tabStock Entry Detail` sed ON sed.parent = se.name
		WHERE se.docstatus = 1
			AND se.work_order IN %(work_orders)s
			AND COALESCE(sed.batch_no, '') != ''

		UNION

		SELECT DISTINCT se.work_order, sbe.batch_no
		FROM `tabStock Entry` se
		INNER JOIN `tabStock Entry Detail` sed ON sed.parent = se.name
		INNER JOIN `tabSerial and Batch Entry` sbe ON sbe.parent = sed.serial_and_batch_bundle
		WHERE se.docstatus = 1
			AND se.work_order IN %(work_orders)s
			AND COALESCE(sed.serial_and_batch_bundle, '') != ''
			AND COALESCE(sbe.batch_no, '') != ''
		""",
		{"work_orders": work_orders_tuple},
		as_dict=True,
	)

	result: dict[str, set[str]] = {}
	for row in rows:
		result.setdefault(row.work_order, set()).add(row.batch_no)

	return result


def get_shift_key(shift_name: str | None) -> str | None:
	if not shift_name:
		return None

	value = shift_name.strip().lower()
	if "1" in value or value.endswith("one"):
		return "shift_1_qty"
	if "2" in value or value.endswith("two"):
		return "shift_2_qty"
	if "3" in value or value.endswith("three"):
		return "shift_3_qty"

	return None

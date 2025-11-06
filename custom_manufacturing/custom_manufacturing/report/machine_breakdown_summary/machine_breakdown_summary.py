from __future__ import annotations

import frappe
from frappe import _


def execute(filters: dict | None = None):
	filters = frappe._dict(filters or {})
	columns = get_columns()
	data = get_data(filters)
	summary = build_summary(data)
	chart = build_chart(data)
	return columns, data, summary, chart


def get_columns() -> list[dict]:
	return [
		{
			"label": _("Workstation"),
			"fieldname": "workstation",
			"fieldtype": "Link",
			"options": "Workstation",
			"width": 200,
		},
		{
			"label": _("Breakdown Type"),
			"fieldname": "breakdown_type",
			"fieldtype": "Link",
			"options": "Breakdown Type",
			"width": 200,
		},
		{
			"label": _("Breakdown Count"),
			"fieldname": "breakdown_count",
			"fieldtype": "Int",
			"width": 140,
		},
	]


def get_data(filters: frappe._dict) -> list[dict]:
	conditions: list[str] = [
		"docstatus < 2",
		"COALESCE(custom_breakdown_type, '') != ''",
		"COALESCE(workstation, '') != ''",
	]

	values: dict[str, object] = {}

	if filters.get("from_date"):
		conditions.append("posting_date >= %(from_date)s")
		values["from_date"] = filters.from_date

	if filters.get("to_date"):
		conditions.append("posting_date <= %(to_date)s")
		values["to_date"] = filters.to_date

	where_clause = " AND ".join(conditions)

	rows = frappe.db.sql(
		f"""
		SELECT
			workstation,
			custom_breakdown_type AS breakdown_type,
			COUNT(*) AS breakdown_count
		FROM `tabJob Card`
		WHERE {where_clause}
		GROUP BY workstation, custom_breakdown_type
		ORDER BY workstation ASC, breakdown_count DESC
		""",
		values,
		as_dict=True,
	)

	return rows


def build_summary(data: list[dict]) -> list[dict] | None:
	if not data:
		return None

	total_breakdowns = sum(int(row.get("breakdown_count") or 0) for row in data)
	machines_impacted = len({row.get("workstation") for row in data if row.get("workstation")})
	breakdown_variety = len({row.get("breakdown_type") for row in data if row.get("breakdown_type")})

	return [
		{
			"value": total_breakdowns,
			"label": _("Total Breakdowns"),
			"indicator": "Red" if total_breakdowns else "Green",
		},
		{
			"value": machines_impacted,
			"label": _("Machines Impacted"),
			"indicator": "Blue",
		},
		{
			"value": breakdown_variety,
			"label": _("Breakdown Types"),
			"indicator": "Green",
		},
	]


def build_chart(data: list[dict]) -> dict | None:
	if not data:
		return None

	by_machine: dict[str, int] = {}
	by_breakdown: dict[str, int] = {}
	for row in data:
		machine = row.get("workstation")
		breakdown = row.get("breakdown_type")
		count = int(row.get("breakdown_count") or 0)

		if machine:
			by_machine[machine] = by_machine.get(machine, 0) + count
		if breakdown:
			by_breakdown[breakdown] = by_breakdown.get(breakdown, 0) + count

	if not by_machine:
		return None

	top_machines = sorted(by_machine.items(), key=lambda item: item[1], reverse=True)[:10]
	machine_labels = [name for name, _ in top_machines]
	machine_counts = [count for _, count in top_machines]

	legend_note = ""
	if len(by_machine) > 10:
		legend_note = _(" (Top 10)")

	overall_breakdowns = sum(by_breakdown.values())

	return {
		"data": {
			"labels": machine_labels,
			"datasets": [
				{
					"name": _("Breakdowns") + legend_note,
					"values": machine_counts,
				}
			],
		},
		"type": "bar",
		"colors": ["#FF6B6B"],
		"barOptions": {"spaceRatio": 0.4},
		"valuesOverPoints": True,
		"fieldtype": "Int",
		"yAxis": [
			{
				"title": _("Breakdown Count"),
			}
		],
		"title": _("Total Breakdowns: {0}").format(overall_breakdowns),
	}

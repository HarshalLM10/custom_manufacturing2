from __future__ import annotations

from typing import Iterable, Sequence

import frappe


def recompute(lead: str | Sequence[str] | None = None, statuses: Iterable[str] | None = None) -> list[str]:
	"""Re-evaluate Lead status for the provided records.

	Args:
	    lead: Optionally pass a single lead name or a list/tuple of names. When omitted,
	        all leads filtered by *statuses* are recalculated.
	    statuses: Optional list of statuses to filter when *lead* is not provided.

	Returns:
	    List of lead names whose status changed during recomputation.
	"""
	if isinstance(lead, str):
		targets = [lead]
	elif lead:
		targets = list(lead)
	else:
		statuses = tuple(statuses) if statuses else ("Opportunity",)
		targets = frappe.get_all(
			"Lead",
			filters={"status": ["in", statuses], "docstatus": ["<", 2]},
			pluck="name",
		)

	updated: list[str] = []
	for lead_name in targets:
		doc = frappe.get_doc("Lead", lead_name)
		if doc.docstatus == 2:
			continue

		prev_status = doc.status
		doc.set_status(update=True)
		doc.reload()

		if doc.status != prev_status:
			updated.append(doc.name)

	if updated:
		frappe.db.commit()

	return updated

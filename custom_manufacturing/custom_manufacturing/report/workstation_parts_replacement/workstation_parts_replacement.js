frappe.query_reports["Workstation Parts Replacement"] = {
    filters: [
        {
            fieldname: "plant",
            label: __("Plant"),
            fieldtype: "Link",
            options: "Plant Floor",
        },
    ],

    formatter(value, row, column, data, default_formatter) {
        if (column.fieldname === "status" && value === __("Parts Replacement Required")) {
            value = `<span class="label label-danger">${value}</span>`;
        }
        return default_formatter(value, row, column, data);
    },
}; 

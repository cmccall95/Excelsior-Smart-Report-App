window.myNamespace = window.myNamespace || {};  // this makes sure that myNamespace exists on the window object
window.myNamespace.tabulator = window.myNamespace.tabulator || {};  // this makes sure that tabulator exists on myNamespace

window.myNamespace.tabulator = Object.assign(window.myNamespace.tabulator, {
    printTable: function (tableData, columns) {
        var doc = new jsPDF('l', 'pt', 'tabloid');

        var columnNames = columns.map(col => col.title); // Adjust as needed depending on the structure of 'columns'
        var data = tableData.map(obj => Object.values(obj));

        doc.autoTable(columnNames, data, {
            styles: {
                overflow: 'linebreak',
                fontSize: 10,
                tableWidth: 'auto'
            },
            columnStyles: {
                1: { columnWidth: 'auto' }
            }
        });

        doc.save('table.pdf');
    },

    triggerPrint: function (tableData, columns) {
        this.printTable(tableData, columns);
    }
});

window.myNamespace.tabulator.headerMenu = [
    {
        label: "Hide Column",
        action: function (e, column) {
            column.hide();
        }
    },
];

// Now we can assign properties to tabulator
window.myNamespace.tabulator.weldLogRowFormatter = function (row) {
    var data = row.getData();
    if (data['_error'] === 'Y') {
        row.getElement().style.backgroundColor = "#FFC7CE";
    }
};

window.myNamespace.tabulator.rtRowFormatter = function (row) {
    var data = row.getData();
    if (data['21'] === 'yes') {
        row.getElement().style.backgroundColor = "#FFC7CE";
    }
};

window.myNamespace.tabulator.rtStatusFormatter = function (cell, formatterParams, onRendered) {
    var value = cell.getValue();
    var cellElement = cell.getElement();

    if (value === "Lot Good" || value === "OK") {
        cellElement.style.backgroundColor = "#E2EFDA";
    } else if (value === "Current Lot" || value === "Current") {
        cellElement.style.backgroundColor = "#DDEBF7";
    } else if (value === "Needs RT" || value === "Needs RT/PT" || value === "Need NDE" || value === "Need" || value === "Needs GAP" || value === "Need Gap") {
        cellElement.style.backgroundColor = "#FFC7CE";
    }
    return value;
};

window.myNamespace.tabulator.xValueFormatter = function (cell, formatterParams, onRendered) {
    var value = cell.getValue();
    var cellElement = cell.getElement();

    //console.log("Formatter is running for cell value: ", value);

    if (value === "X") {
        cellElement.classList.add("highlight-cell");
    }
    return value;
};
